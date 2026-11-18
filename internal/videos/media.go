package videos

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"html"
	"math/big"
	"os"
	"os/exec"
	"strconv"
	"strings"
)

// Probe is what the delivered file says about itself. Billing reads these,
// not the requested seconds: they are what the customer received (design 7.1).
type Probe struct {
	Frames int
	// FPS is the stream's frame rate as an exact fraction.
	FPS *big.Rat
}

// OutputSeconds is frames / fps as a decimal string rounded half up to four
// places (the column is NUMERIC(12,4)): 81 frames at 16 fps is "5.0625", 121
// at 24 is "5.0417".
func (p Probe) OutputSeconds() string {
	if p.FPS == nil || p.FPS.Sign() <= 0 {
		return "0"
	}
	secs := new(big.Rat).Quo(new(big.Rat).SetInt64(int64(p.Frames)), p.FPS)
	return roundRat(secs, 4)
}

// roundRat renders r rounded half up to places decimals, without trailing
// zeros beyond what is needed ("5.0625", "5", "5.0417").
func roundRat(r *big.Rat, places int) string {
	scale := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(places)), nil)
	num := new(big.Int).Mul(r.Num(), scale)
	q, m := new(big.Int).QuoRem(num, r.Denom(), new(big.Int))
	if new(big.Int).Mul(m, big.NewInt(2)).Cmp(r.Denom()) >= 0 {
		q.Add(q, big.NewInt(1))
	}
	s := q.String()
	for len(s) <= places {
		s = "0" + s
	}
	intPart, frac := s[:len(s)-places], strings.TrimRight(s[len(s)-places:], "0")
	if frac == "" {
		return intPart
	}
	return intPart + "." + frac
}

// MediaTool reads and thumbnails a video file.
type MediaTool interface {
	ProbeFile(ctx context.Context, path string) (Probe, error)
	Thumbnail(ctx context.Context, videoPath, outPath string) error
}

// Media runs ffprobe and ffmpeg (shipped in the phoebe image).
type Media struct {
	FFprobe string
	FFmpeg  string
}

func (m Media) bin(name, set string) string {
	if set != "" {
		return set
	}
	return name
}

// ProbeFile reads the first video stream's frame count and frame rate.
func (m Media) ProbeFile(ctx context.Context, path string) (Probe, error) {
	out, err := exec.CommandContext(ctx, m.bin("ffprobe", m.FFprobe), "-v", "error",
		"-select_streams", "v:0", "-count_packets",
		"-show_entries", "stream=codec_type,nb_frames,nb_read_packets,r_frame_rate,avg_frame_rate",
		"-of", "json", path).Output()
	if err != nil {
		return Probe{}, fmt.Errorf("ffprobe: %w", err)
	}
	var parsed struct {
		Streams []struct {
			CodecType     string `json:"codec_type"`
			NbFrames      string `json:"nb_frames"`
			NbReadPackets string `json:"nb_read_packets"`
			RFrameRate    string `json:"r_frame_rate"`
			AvgFrameRate  string `json:"avg_frame_rate"`
		} `json:"streams"`
	}
	if err := json.Unmarshal(out, &parsed); err != nil || len(parsed.Streams) == 0 {
		return Probe{}, fmt.Errorf("ffprobe: no video stream")
	}
	s := parsed.Streams[0]
	frames, err := strconv.Atoi(s.NbFrames)
	if err != nil || frames <= 0 {
		frames, err = strconv.Atoi(s.NbReadPackets)
		if err != nil || frames <= 0 {
			return Probe{}, fmt.Errorf("ffprobe: no frame count")
		}
	}
	rate := s.AvgFrameRate
	if rate == "" || rate == "0/0" {
		rate = s.RFrameRate
	}
	fps, ok := new(big.Rat).SetString(rate)
	if !ok || fps.Sign() <= 0 {
		return Probe{}, fmt.Errorf("ffprobe: frame rate %q", rate)
	}
	return Probe{Frames: frames, FPS: fps}, nil
}

// Thumbnail writes the first frame as WebP.
func (m Media) Thumbnail(ctx context.Context, videoPath, outPath string) error {
	cmd := exec.CommandContext(ctx, m.bin("ffmpeg", m.FFmpeg), "-v", "error", "-y", "-i", videoPath,
		"-frames:v", "1", "-vf", "scale='min(640,iw)':-2", "-c:v", "libwebp", "-quality", "80", outPath)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("ffmpeg thumbnail: %w: %s", err, strings.TrimSpace(stderr.String()))
	}
	return nil
}

// xmpUUID is the ISO base media file format extended type of an XMP packet
// box (Adobe XMP Specification Part 3, "MP4": uuid BE7ACFCB-97A9-42E8-9C71-
// 999491E3AFAC at the top level of the file).
var xmpUUID = []byte{0xBE, 0x7A, 0xCF, 0xCB, 0x97, 0xA9, 0x42, 0xE8, 0x9C, 0x71, 0x99, 0x94, 0x91, 0xE3, 0xAF, 0xAC}

// DigitalSourceType is the IPTC term for media made by a trained model.
const DigitalSourceType = "http://cv.iptc.org/newscodes/digitalsourcetype/trainedAlgorithmicMedia"

// XMPPacket is the label every generated video carries (design 6.4): the IPTC
// digital source type trainedAlgorithmicMedia (EU AI Act Article 50(2) asks
// for machine-readable marking), the model and the job id.
func XMPPacket(model, jobID string) []byte {
	esc := html.EscapeString
	return []byte(`<?xpacket begin="` + "\ufeff" + `" id="W5M0MpCehiHzreSzNTczkc9d"?>` +
		`<x:xmpmeta xmlns:x="adobe:ns:meta/"><rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">` +
		`<rdf:Description rdf:about=""` +
		` xmlns:Iptc4xmpExt="http://iptc.org/std/Iptc4xmpExt/2008-02-29/"` +
		` xmlns:xmp="http://ns.adobe.com/xap/1.0/"` +
		` xmlns:xmpMM="http://ns.adobe.com/xap/1.0/mm/"` +
		` xmlns:dc="http://purl.org/dc/elements/1.1/"` +
		` Iptc4xmpExt:DigitalSourceType="` + DigitalSourceType + `"` +
		` xmp:CreatorTool="Saturn Cloud Token Factory: ` + esc(model) + `"` +
		` xmpMM:DocumentID="saturn:` + esc(jobID) + `">` +
		`<dc:description><rdf:Alt><rdf:li xml:lang="x-default">AI-generated video. Model: ` + esc(model) +
		`. Job: ` + esc(jobID) + `.</rdf:li></rdf:Alt></dc:description>` +
		`</rdf:Description></rdf:RDF></x:xmpmeta><?xpacket end="w"?>`)
}

// AppendXMP appends an XMP uuid box to the end of an MP4 file. A top-level box
// after the others is valid ISO BMFF; players skip boxes they do not know, and
// no offset inside the file moves, so the video plays exactly as before.
func AppendXMP(path string, packet []byte) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	size := 8 + len(xmpUUID) + len(packet)
	hdr := make([]byte, 8)
	binary.BigEndian.PutUint32(hdr[:4], uint32(size))
	copy(hdr[4:], "uuid")
	for _, part := range [][]byte{hdr, xmpUUID, packet} {
		if _, err := f.Write(part); err != nil {
			return err
		}
	}
	return f.Sync()
}

// gpuSeconds is gpus x wall seconds, three decimals.
func gpuSeconds(gpus int, wallMillis int64) string {
	r := new(big.Rat).SetFrac(big.NewInt(int64(gpus)*wallMillis), big.NewInt(1000))
	return roundRat(r, 3)
}
