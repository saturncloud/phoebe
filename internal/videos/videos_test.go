package videos

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"io"
	"math/big"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/saturncloud/phoebe/internal/logging"
)

// A 1x1 PNG.
var pngBytes, _ = base64.StdEncoding.DecodeString(
	"iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR4nGNgYGD4DwABBAEAwS2OUAAAAABJRU5ErkJggg==")

var testLimits = Limits{Tasks: []string{TaskT2V, TaskI2V}, Sizes: []string{"1280x704", "704x1280"},
	Seconds: []int{5}, FPS: 24, MaxSteps: 50, MaxConcurrency: 1, JobsInFlight: 20, GPUs: 1}

func TestNewIDIsAValidSortableULID(t *testing.T) {
	a, err := NewID(time.UnixMilli(1763400000000))
	if err != nil {
		t.Fatal(err)
	}
	b, _ := NewID(time.UnixMilli(1763400000001))
	if !ValidID(a) || !ValidID(b) {
		t.Fatalf("ids not valid: %q %q", a, b)
	}
	if len(a) != len("video_")+26 {
		t.Fatalf("id %q has the wrong length", a)
	}
	if !(a[:16] < b[:16]) {
		t.Fatalf("ids do not sort by time: %q %q", a, b)
	}
}

func TestValidIDRefusesMalformedIDs(t *testing.T) {
	for _, tc := range []struct{ id, reason string }{
		{"", "empty"},
		{"video_", "no ulid"},
		{"video_01ARZ3NDEKTSV4RRFFQ69G5FA", "25 characters"},
		{"video_01ARZ3NDEKTSV4RRFFQ69G5FAVX", "27 characters"},
		{"video_01arz3ndektsv4rrffq69g5fav", "lower case"},
		{"video_81ARZ3NDEKTSV4RRFFQ69G5FAV", "first character over 7 overflows 128 bits"},
		{"video_01ARZ3NDEKTSV4RRFFQ69G5FAI", "I is not Crockford"},
		{"vid_01ARZ3NDEKTSV4RRFFQ69G5FAV", "wrong prefix"},
		{"video_01ARZ3NDEKTSV4RRFFQ69G5FA/", "path character"},
	} {
		if ValidID(tc.id) {
			t.Errorf("ValidID(%q) = true, want false (%s)", tc.id, tc.reason)
		}
	}
	if !ValidID("video_01ARZ3NDEKTSV4RRFFQ69G5FAV") {
		t.Error("a well-formed id was refused")
	}
}

func TestResolutionBucketsByTheShortSide(t *testing.T) {
	for _, tc := range []struct{ size, want string }{
		{"832x480", "480p"}, {"480x832", "480p"}, {"1280x720", "720p"}, {"1280x704", "720p"},
		{"720x1280", "720p"}, {"1920x1080", "1080p"}, {"1080x1920", "1080p"}, {"3840x2160", "4k"},
		{"1081x1920", "4k"},
	} {
		got, err := Resolution(tc.size)
		if err != nil || got != tc.want {
			t.Errorf("Resolution(%q) = %q, %v; want %q", tc.size, got, err, tc.want)
		}
	}
	for _, bad := range []string{"", "1280", "x720", "1280x", "-1x720", "axb", "9000x9000"} {
		if _, err := Resolution(bad); err == nil {
			t.Errorf("Resolution(%q) accepted a malformed size", bad)
		}
	}
}

func TestParseLimitsFailsClosed(t *testing.T) {
	good := `{"tasks":["t2v"],"sizes":["1280x720"],"seconds":[5],"fps":16,"max_steps":40,"max_concurrency":1,"jobs_in_flight":0,"gpus":8}`
	l, err := ParseLimits(good)
	if err != nil {
		t.Fatal(err)
	}
	if l.JobsInFlight != DefaultJobsInFlight {
		t.Errorf("jobs_in_flight 0 should mean the default %d, got %d", DefaultJobsInFlight, l.JobsInFlight)
	}
	if l.Frames(5) != 81 {
		t.Errorf("5 s at 16 fps should be 81 frames (Wan's 4k+1), got %d", l.Frames(5))
	}
	for _, tc := range []struct{ raw, reason string }{
		{``, "empty"},
		{`{}`, "nothing"},
		{strings.Replace(good, `"t2v"`, `"v2v"`, 1), "unknown task"},
		{strings.Replace(good, `"1280x720"`, `"big"`, 1), "bad size"},
		{strings.Replace(good, `"fps":16`, `"fps":0`, 1), "zero fps"},
		{strings.Replace(good, `"gpus":8`, `"gpus":0`, 1), "zero gpus"},
		{strings.Replace(good, `"max_steps":40`, `"max_steps":40,"lora":"x"`, 1), "unknown field"},
		{strings.Replace(good, `[5]`, `[]`, 1), "no seconds"},
	} {
		if _, err := ParseLimits(tc.raw); err == nil {
			t.Errorf("ParseLimits accepted %s: %s", tc.reason, tc.raw)
		}
	}
}

func jsonRequest(body string) *http.Request {
	r := httptest.NewRequest(http.MethodPost, "/v1/videos", strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	return r
}

func TestParseCreateValidatesEveryField(t *testing.T) {
	dataURL := "data:image/png;base64," + base64.StdEncoding.EncodeToString(pngBytes)
	for _, tc := range []struct {
		body   string
		status int
		param  string
		reason string
	}{
		{`{"model":"m","prompt":"a cat"}`, 0, "", "minimal request uses the defaults"},
		{`{"model":"m","prompt":"a cat","seconds":"5"}`, 0, "", "seconds as a string (OpenAI)"},
		{`{"model":"m","prompt":"a cat","seconds":5}`, 0, "", "seconds as an integer"},
		{`{"model":"m","prompt":"a cat","seconds":4}`, 400, "seconds", "seconds not offered"},
		{`{"model":"m","prompt":"a cat","size":"1920x1080"}`, 400, "size", "size not offered"},
		{`{"model":"m","prompt":""}`, 400, "prompt", "empty prompt"},
		{`{"model":"m"}`, 400, "prompt", "missing prompt"},
		{`{"prompt":"a cat"}`, 400, "model", "missing model"},
		{`{"model":"m","prompt":"a cat","flow_shift":3}`, 400, "flow_shift", "unknown field named"},
		{`{"model":"m","prompt":"a cat","lora":"x"}`, 400, "lora", "a cost-changing engine knob"},
		{`{"model":"m","prompt":"a cat","num_inference_steps":51}`, 400, "num_inference_steps", "over the catalog cap"},
		{`{"model":"m","prompt":"a cat","num_inference_steps":50,"seed":7,"guidance_scale":5,"negative_prompt":"blur"}`, 0, "", "every extension"},
		{`{"model":"m","prompt":"a cat","seed":-1}`, 400, "seed", "negative seed"},
		{`{"model":"m","prompt":"a cat","input_reference":{"image_url":"https://example.com/a.png"}}`, 400, "input_reference", "HTTP URL refused (phoebe never fetches)"},
		{`{"model":"m","prompt":"a cat","input_reference":{"image_url":"HTTP://EXAMPLE.COM/a.png"}}`, 400, "input_reference", "HTTP URL in any case refused"},
		{`{"model":"m","prompt":"a cat","input_reference":{"image_url":"` + dataURL + `"}}`, 0, "", "data URL image"},
		{`{"model":"m","prompt":"a cat","input_reference":{"image_url":"data:text/plain;base64,aGVsbG8="}}`, 400, "input_reference", "not an image"},
		{`{"model":"m","prompt":"a cat","input_reference":{"image_url":"data:image/png;base64,aGVsbG8="}}`, 400, "input_reference", "claims png, is not"},
		{`[1]`, 400, "", "not an object"},
		{`{"model":"m","prompt":"a cat"} {}`, 400, "", "trailing value"},
	} {
		req, err := ParseCreate(jsonRequest(tc.body), testLimits)
		if tc.status == 0 {
			if err != nil {
				t.Errorf("%s: refused: %v", tc.reason, err)
			}
			continue
		}
		if err == nil || err.Status != tc.status || err.Param != tc.param {
			t.Errorf("%s: got %+v, %+v; want %d param %q", tc.reason, req, err, tc.status, tc.param)
		}
	}
}

func TestParseCreateDefaultsAndTask(t *testing.T) {
	req, err := ParseCreate(jsonRequest(`{"model":"m","prompt":"a cat"}`), testLimits)
	if err != nil {
		t.Fatal(err)
	}
	if req.Seconds != 5 || req.Size != "1280x704" || req.Task != TaskT2V {
		t.Errorf("defaults: %+v", req)
	}
	t2vOnly := testLimits
	t2vOnly.Tasks = []string{TaskT2V}
	dataURL := "data:image/png;base64," + base64.StdEncoding.EncodeToString(pngBytes)
	_, err = ParseCreate(jsonRequest(`{"model":"m","prompt":"a","input_reference":{"image_url":"`+dataURL+`"}}`), t2vOnly)
	if err == nil || err.Param != "input_reference" {
		t.Errorf("a text-to-video model took an image: %+v", err)
	}
	i2vOnly := testLimits
	i2vOnly.Tasks = []string{TaskI2V}
	if _, err := ParseCreate(jsonRequest(`{"model":"m","prompt":"a"}`), i2vOnly); err == nil {
		t.Error("an image-to-video model accepted no image")
	}
}

func multipartRequest(t *testing.T, fields map[string]string, image []byte) *http.Request {
	t.Helper()
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	for k, v := range fields {
		_ = mw.WriteField(k, v)
	}
	if image != nil {
		fw, _ := mw.CreateFormFile("input_reference", "in.png")
		_, _ = fw.Write(image)
	}
	_ = mw.Close()
	r := httptest.NewRequest(http.MethodPost, "/v1/videos", &buf)
	r.Header.Set("Content-Type", mw.FormDataContentType())
	return r
}

func TestParseCreateMultipart(t *testing.T) {
	req, err := ParseCreate(multipartRequest(t, map[string]string{"model": "m", "prompt": "a", "seconds": "5"}, pngBytes), testLimits)
	if err != nil {
		t.Fatal(err)
	}
	if req.Task != TaskI2V || req.Params.InputContentType != "image/png" || !bytes.Equal(req.Input, pngBytes) {
		t.Errorf("multipart image: %+v", req)
	}
	if _, err := ParseCreate(multipartRequest(t, map[string]string{"model": "m", "prompt": "a", "steps": "3"}, nil), testLimits); err == nil || err.Param != "steps" {
		t.Errorf("unknown multipart field: %+v", err)
	}
	big := bytes.Repeat([]byte{0}, MaxInputImageBytes+1)
	copy(big, pngBytes)
	if _, err := ParseCreate(multipartRequest(t, map[string]string{"model": "m", "prompt": "a"}, big), testLimits); err == nil || err.Status != http.StatusRequestEntityTooLarge {
		t.Errorf("an image over 10 MiB: %+v", err)
	}
	if _, err := ParseCreate(multipartRequest(t, map[string]string{"model": "m", "prompt": "a", "input_reference": "https://x/y.png"}, nil), testLimits); err == nil || err.Param != "input_reference" {
		t.Errorf("an HTTP URL as a multipart text field: %+v", err)
	}
}

func TestOutputSecondsIsFramesOverFPS(t *testing.T) {
	for _, tc := range []struct {
		frames int
		fps    string
		want   string
	}{
		{81, "16/1", "5.0625"}, // design 7.1's example
		{121, "24/1", "5.041666667"},
		{120, "24/1", "5"},
		{1, "3/1", "0.333333333"},
		{2, "3/1", "0.666666667"},
		{30000, "30000/1001", "1001"},
	} {
		fps, _ := new(big.Rat).SetString(tc.fps)
		if got := (Probe{Frames: tc.frames, FPS: fps}).OutputSeconds(); got != tc.want {
			t.Errorf("%d frames at %s = %s, want %s", tc.frames, tc.fps, got, tc.want)
		}
	}
	if got := gpuSeconds(8, 250712); got != "2005.696" {
		t.Errorf("gpuSeconds(8, 250.712 s) = %s", got)
	}
}

func TestAppendXMPAddsOneTopLevelUUIDBoxWithTheLabel(t *testing.T) {
	path := filepath.Join(t.TempDir(), "v.mp4")
	orig := []byte("\x00\x00\x00\x08free")
	if err := os.WriteFile(path, orig, 0o600); err != nil {
		t.Fatal(err)
	}
	packet := XMPPacket("Wan-AI/Wan2.2-TI2V-5B-Diffusers", "video_01ARZ3NDEKTSV4RRFFQ69G5FAV")
	if err := AppendXMP(path, packet); err != nil {
		t.Fatal(err)
	}
	got, _ := os.ReadFile(path)
	if !bytes.HasPrefix(got, orig) {
		t.Fatal("the original bytes moved")
	}
	box := got[len(orig):]
	if int(binary.BigEndian.Uint32(box[:4])) != len(box) || string(box[4:8]) != "uuid" || !bytes.Equal(box[8:24], xmpUUID) {
		t.Fatalf("bad box header %x", box[:24])
	}
	xmp := string(box[24:])
	for _, want := range []string{DigitalSourceType, "Wan-AI/Wan2.2-TI2V-5B-Diffusers", "video_01ARZ3NDEKTSV4RRFFQ69G5FAV", "AI-generated"} {
		if !strings.Contains(xmp, want) {
			t.Errorf("label lacks %q", want)
		}
	}
	if strings.Contains(string(XMPPacket(`a"<b>`, "x")), `a"<b>`) {
		t.Error("model name not escaped in the XMP")
	}
}

func TestOmniFormAsksForFramesAndPassesOnlyAllowedKnobs(t *testing.T) {
	steps, seed := 30, int64(9)
	prompt := "a cat"
	job := &Job{Prompt: &prompt, Size: "1280x704", Seconds: 5, FPS: 24,
		Params: Params{NumInferenceSteps: &steps, Seed: &seed, InputContentType: "image/png"}}
	body, ctype, err := omniForm(job, pngBytes)
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest(http.MethodPost, "/", body)
	r.Header.Set("Content-Type", ctype)
	if err := r.ParseMultipartForm(1 << 20); err != nil {
		t.Fatal(err)
	}
	for k, want := range map[string]string{"prompt": "a cat", "num_frames": "121", "fps": "24",
		"width": "1280", "height": "704", "num_inference_steps": "30", "seed": "9", "size": "1280x704"} {
		if got := r.FormValue(k); got != want {
			t.Errorf("%s = %q, want %q", k, got, want)
		}
	}
	if _, ok := r.MultipartForm.Value["guidance_scale"]; ok {
		t.Error("an unset knob was sent")
	}
	f, _, err := r.FormFile("input_reference")
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(f)
	if !bytes.Equal(got, pngBytes) {
		t.Error("input image not passed")
	}
}

func TestEngineMessageNeverEchoesTheEngineText(t *testing.T) {
	msg := engineMessage(&EngineHTTPError{Status: 500, Body: "Traceback /opt/venv/secret"})
	if strings.Contains(msg, "secret") || !strings.Contains(msg, "500") {
		t.Errorf("engineMessage = %q", msg)
	}
}

func TestHandoffRoundTripsAndRefusesIncompleteIdentity(t *testing.T) {
	h := Handoff{OrgID: "o", AuthID: "a", ResourceID: "r", ServedModel: "m", BaseModel: "b",
		ServingMode: "dedicated", Upstream: "u:8000", Limits: "{}"}
	enc, err := EncodeHandoff(h)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := decodeHandoff(enc); err != nil || got != h {
		t.Fatalf("round trip: %+v %v", got, err)
	}
	for _, mut := range []func(*Handoff){
		func(h *Handoff) { h.OrgID = "" }, func(h *Handoff) { h.AuthID = "" },
		func(h *Handoff) { h.ResourceID = "" }, func(h *Handoff) { h.ServingMode = "shared" },
		func(h *Handoff) { h.Upstream = "" }, func(h *Handoff) { h.ServedModel = "" },
	} {
		bad := h
		mut(&bad)
		enc, _ := EncodeHandoff(bad)
		if _, err := decodeHandoff(enc); err == nil {
			t.Errorf("accepted incomplete identity %+v", bad)
		}
	}
	if _, err := decodeHandoff("not base64!"); err == nil {
		t.Error("accepted garbage")
	}
}

func TestAPIRefusesRequestsWithoutTheInterceptorToken(t *testing.T) {
	api := NewAPI(APIConfig{Token: "secret"}, nil, nil, nil, testLogger())
	for _, token := range []string{"", "wrong", "secre"} {
		r := httptest.NewRequest(http.MethodGet, "/v1/videos", nil)
		if token != "" {
			r.Header.Set(HeaderToken, token)
		}
		w := httptest.NewRecorder()
		api.ServeHTTP(w, r)
		if w.Code != http.StatusNotFound {
			t.Errorf("token %q: status %d, want 404", token, w.Code)
		}
	}
	// An API with no token configured refuses everything, even an empty token.
	empty := NewAPI(APIConfig{}, nil, nil, nil, testLogger())
	w := httptest.NewRecorder()
	empty.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/v1/videos", nil))
	if w.Code != http.StatusNotFound {
		t.Errorf("unconfigured token: %d", w.Code)
	}
}

func testLogger() *logging.Logger { return logging.New(logging.ERROR) }
