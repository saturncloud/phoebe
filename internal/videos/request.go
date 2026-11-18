package videos

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"
)

const (
	// MaxPromptChars bounds the prompt (design 3.4).
	MaxPromptChars = 8000
	// MaxInputImageBytes bounds an input image (design 3.4); over it is a 413.
	MaxInputImageBytes = 10 << 20
)

// allowedFields is the whole request surface (design 3.4; a one-way door).
// Any other field is a 400 naming it: the engine accepts more knobs, some of
// which change the cost or load files, and an allow-list is easy to widen
// later while silently accepting unknown fields would be hard to take back.
var allowedFields = map[string]bool{
	"model": true, "prompt": true, "seconds": true, "size": true, "input_reference": true,
	"negative_prompt": true, "seed": true, "num_inference_steps": true, "guidance_scale": true,
}

// imageTypes are the input image media types accepted, by sniffed content.
var imageTypes = map[string]string{"image/png": "png", "image/jpeg": "jpg", "image/webp": "webp"}

// CreateRequest is a validated POST /v1/videos.
type CreateRequest struct {
	Model   string
	Prompt  string
	Seconds int
	Size    string
	Task    string
	Params  Params
	// Input is the decoded input image (i2v), with its media type in
	// Params.InputContentType.
	Input []byte
}

// ParseCreate reads and validates a create request against the endpoint's
// limits. JSON and multipart/form-data are both accepted (the OpenAI SDK sends
// multipart when it uploads input_reference as a file).
func ParseCreate(r *http.Request, limits Limits) (*CreateRequest, *APIError) {
	mediaType, params, _ := mime.ParseMediaType(r.Header.Get("Content-Type"))
	fields := map[string]any{}
	var input []byte
	switch mediaType {
	case "multipart/form-data":
		mr := multipart.NewReader(r.Body, params["boundary"])
		for {
			part, err := mr.NextPart()
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, badRequest("", "malformed multipart body")
			}
			name := part.FormName()
			if !allowedFields[name] {
				return nil, badRequest(name, fmt.Sprintf("unknown field %q", name))
			}
			if _, dup := fields[name]; dup || (name == "input_reference" && input != nil) {
				return nil, badRequest(name, fmt.Sprintf("field %q given twice", name))
			}
			data, err := io.ReadAll(io.LimitReader(part, MaxInputImageBytes+1))
			if err != nil {
				return nil, badRequest(name, "malformed multipart body")
			}
			if name == "input_reference" {
				if len(data) > MaxInputImageBytes {
					return nil, inputTooLarge()
				}
				if part.FileName() == "" {
					// A text part: the JSON form of input_reference, or a URL.
					var ref any
					if json.Unmarshal(data, &ref) != nil {
						ref = string(data)
					}
					fields[name] = ref
					continue
				}
				input = data
				fields[name] = nil
				continue
			}
			fields[name] = string(data)
		}
	case "application/json", "":
		body, err := io.ReadAll(r.Body)
		if err != nil {
			return nil, badRequest("", "could not read the request body")
		}
		dec := json.NewDecoder(bytes.NewReader(body))
		dec.UseNumber()
		if err := dec.Decode(&fields); err != nil {
			return nil, badRequest("", "the request body is not a JSON object")
		}
		if dec.More() {
			return nil, badRequest("", "the request body is not a JSON object")
		}
		unknown := []string{}
		for k := range fields {
			if !allowedFields[k] {
				unknown = append(unknown, k)
			}
		}
		if len(unknown) > 0 {
			sort.Strings(unknown)
			return nil, badRequest(unknown[0], fmt.Sprintf("unknown field %q", unknown[0]))
		}
	default:
		return nil, badRequest("", "Content-Type must be application/json or multipart/form-data")
	}

	req := &CreateRequest{}
	var e *APIError
	if req.Model, e = stringField(fields, "model", true); e != nil {
		return nil, e
	}
	if req.Prompt, e = stringField(fields, "prompt", true); e != nil {
		return nil, e
	}
	if n := utf8.RuneCountInString(req.Prompt); n < 1 || n > MaxPromptChars {
		return nil, badRequest("prompt", fmt.Sprintf("prompt must be 1 to %d characters", MaxPromptChars))
	}

	req.Seconds = limits.Seconds[0]
	if v, ok := fields["seconds"]; ok && v != nil {
		n, ok := intValue(v)
		if !ok || !containsInt(limits.Seconds, n) {
			return nil, badRequest("seconds", fmt.Sprintf("seconds must be one of %s", joinInts(limits.Seconds)))
		}
		req.Seconds = n
	}
	req.Size = limits.Sizes[0]
	if v, ok := fields["size"]; ok && v != nil {
		s, isStr := v.(string)
		if !isStr || !containsString(limits.Sizes, s) {
			return nil, badRequest("size", fmt.Sprintf("size must be one of %s", strings.Join(limits.Sizes, ", ")))
		}
		req.Size = s
	}

	if v, ok := fields["negative_prompt"]; ok && v != nil {
		s, isStr := v.(string)
		if !isStr || utf8.RuneCountInString(s) > MaxPromptChars {
			return nil, badRequest("negative_prompt", fmt.Sprintf("negative_prompt must be a string of at most %d characters", MaxPromptChars))
		}
		req.Params.NegativePrompt = &s
	}
	if v, ok := fields["seed"]; ok && v != nil {
		n, ok := int64Value(v)
		if !ok || n < 0 || n > 1<<32-1 {
			return nil, badRequest("seed", "seed must be an integer from 0 to 4294967295")
		}
		req.Params.Seed = &n
	}
	if v, ok := fields["num_inference_steps"]; ok && v != nil {
		n, ok := intValue(v)
		if !ok || n < 1 || n > limits.MaxSteps {
			return nil, badRequest("num_inference_steps", fmt.Sprintf("num_inference_steps must be from 1 to %d", limits.MaxSteps))
		}
		req.Params.NumInferenceSteps = &n
	}
	if v, ok := fields["guidance_scale"]; ok && v != nil {
		f, ok := floatValue(v)
		if !ok || f < 0 || f > 50 {
			return nil, badRequest("guidance_scale", "guidance_scale must be a number from 0 to 50")
		}
		req.Params.GuidanceScale = &f
	}

	// input_reference: a file part, or {"image_url": "data:image/...;base64,..."}.
	// HTTP(S) URLs are refused so phoebe never fetches a customer-supplied URL.
	if ref, ok := fields["input_reference"]; ok {
		if input == nil {
			data, e := decodeImageReference(ref)
			if e != nil {
				return nil, e
			}
			input = data
		}
		ct := http.DetectContentType(input)
		ext, ok := imageTypes[ct]
		if !ok {
			return nil, badRequest("input_reference", "input_reference must be a PNG, JPEG or WebP image")
		}
		_ = ext
		req.Input = input
		req.Params.InputContentType = ct
		req.Task = TaskI2V
	} else {
		req.Task = TaskT2V
	}
	if !limits.HasTask(req.Task) {
		if req.Task == TaskI2V {
			return nil, badRequest("input_reference", "this model does not take an input image")
		}
		return nil, badRequest("input_reference", "this model needs an input image (input_reference)")
	}
	return req, nil
}

func inputTooLarge() *APIError {
	return &APIError{Status: http.StatusRequestEntityTooLarge, Code: "input_too_large",
		Param: "input_reference", Message: fmt.Sprintf("input_reference must be at most %d bytes", MaxInputImageBytes)}
}

func decodeImageReference(ref any) ([]byte, *APIError) {
	var url string
	switch v := ref.(type) {
	case map[string]any:
		if len(v) != 1 {
			return nil, badRequest("input_reference", `input_reference must be {"image_url": "data:..."}`)
		}
		s, ok := v["image_url"].(string)
		if !ok {
			return nil, badRequest("input_reference", `input_reference must be {"image_url": "data:..."}`)
		}
		url = s
	case string:
		url = v
	default:
		return nil, badRequest("input_reference", `input_reference must be a file or {"image_url": "data:..."}`)
	}
	lower := strings.ToLower(url)
	if strings.HasPrefix(lower, "http://") || strings.HasPrefix(lower, "https://") {
		return nil, badRequest("input_reference", "input_reference must be a file or a data: URL; HTTP(S) URLs are not fetched")
	}
	header, payload, ok := strings.Cut(url, ",")
	if !ok || !strings.HasPrefix(lower, "data:image/") || !strings.HasSuffix(strings.ToLower(header), ";base64") {
		return nil, badRequest("input_reference", "input_reference must be a data:image/...;base64, URL")
	}
	if base64.StdEncoding.DecodedLen(len(payload)) > MaxInputImageBytes+3 {
		return nil, inputTooLarge()
	}
	data, err := base64.StdEncoding.DecodeString(payload)
	if err != nil {
		return nil, badRequest("input_reference", "input_reference is not valid base64")
	}
	if len(data) > MaxInputImageBytes {
		return nil, inputTooLarge()
	}
	return data, nil
}

func stringField(fields map[string]any, name string, required bool) (string, *APIError) {
	v, ok := fields[name]
	if !ok || v == nil {
		if required {
			return "", badRequest(name, fmt.Sprintf("%s is required", name))
		}
		return "", nil
	}
	s, isStr := v.(string)
	if !isStr {
		return "", badRequest(name, fmt.Sprintf("%s must be a string", name))
	}
	return s, nil
}

// intValue accepts an integer or a string holding one: OpenAI sends seconds
// as a string ("4"), and multipart fields are always strings.
func intValue(v any) (int, bool) {
	n, ok := int64Value(v)
	if !ok || n > 1<<31-1 || n < -(1<<31) {
		return 0, false
	}
	return int(n), true
}

func int64Value(v any) (int64, bool) {
	switch t := v.(type) {
	case json.Number:
		n, err := t.Int64()
		return n, err == nil
	case string:
		n, err := strconv.ParseInt(strings.TrimSpace(t), 10, 64)
		return n, err == nil
	}
	return 0, false
}

func floatValue(v any) (float64, bool) {
	switch t := v.(type) {
	case json.Number:
		f, err := t.Float64()
		return f, err == nil
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(t), 64)
		return f, err == nil
	}
	return 0, false
}

func containsInt(xs []int, x int) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

func containsString(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

func joinInts(xs []int) string {
	parts := make([]string, len(xs))
	for i, x := range xs {
		parts[i] = strconv.Itoa(x)
	}
	return strings.Join(parts, ", ")
}
