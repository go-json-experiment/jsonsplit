package jsonsplit

import (
	"bytes"
	"encoding"
	"encoding/json"
	"expvar"
	"fmt"
	"math"
	"math/big"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	jsonv2 "github.com/go-json-experiment/json"
	jsontext "github.com/go-json-experiment/json/jsontext"
	jsonv1 "github.com/go-json-experiment/json/v1"
	"github.com/google/go-cmp/cmp"
)

// callerPlus adjusts file:line as file:line+n.
func callerPlus(s string, n int) string {
	if i := strings.LastIndexByte(s, ':') + len(":"); i > 0 {
		if m, err := strconv.Atoi(s[i:]); err == nil {
			s = s[:i] + strconv.Itoa(m+n)
		}
	}
	return s
}

// optsOf joins all the JSON options assuming they were all true.
func optsOf(optFuncs ...func(bool) jsonv2.Options) jsonv2.Options {
	var opts []jsonv2.Options
	for _, opt := range optFuncs {
		opts = append(opts, opt(true))
	}
	return jsonv2.JoinOptions(opts...)
}

func newer[T any]() func() any {
	return func() any { return new(T) }
}

func TestCodecMarshal(t *testing.T) {
	var gotDiff Difference
	var wantMetrics CodecMetrics
	codec := Codec{
		AutoDetectOptions: true,
		ReportDifference: func(d Difference) {
			gotDiff = d
			wantMetrics.NumMarshalDiffs.Add(1)
			wantMetrics.MarshalCallerHistogram.Add(d.Caller, 1)
			for name := range optionNames(d.Options) {
				wantMetrics.MarshalOptionHistogram.Add(name, 1)
			}
		},
	}

	for _, tt := range []struct {
		mode     CallMode
		in       any
		inOpts   jsonv2.Options
		diffOpts jsonv2.Options
	}{{
		mode: OnlyCallV1,
		in:   "\xde\xad\xbe\xef",
	}, {
		mode: CallV1ButUponErrorReturnV2,
		in:   "\xde\xad\xbe\xef",
	}, {
		mode:     CallBothButReturnV1,
		in:       "\xde\xad\xbe\xef",
		diffOpts: optsOf(jsontext.AllowInvalidUTF8),
	}, {
		mode:     CallBothButReturnV2,
		in:       "\xde\xad\xbe\xef",
		diffOpts: optsOf(jsontext.AllowInvalidUTF8),
	}, {
		mode:     CallV2ButUponErrorReturnV1,
		in:       "\xde\xad\xbe\xef",
		diffOpts: optsOf(jsontext.AllowInvalidUTF8),
	}, {
		mode: OnlyCallV2,
		in:   "\xde\xad\xbe\xef",
	}, {
		mode: CallV1ButUponErrorReturnV2,
		in:   "<html>",
	}, {
		mode:     CallBothButReturnV1,
		in:       "<html>",
		diffOpts: optsOf(jsontext.EscapeForHTML),
	}, {
		mode: CallBothButReturnV1,
		in: struct {
			A int `json:",omitempty"`
		}{},
		diffOpts: optsOf(jsonv1.OmitEmptyWithLegacySemantics),
	}, {
		mode: CallBothButReturnV2,
		in: struct {
			A bool `json:",string"`
		}{},
		diffOpts: optsOf(jsonv1.StringifyWithLegacySemantics),
	}, {
		mode:     CallBothButReturnV2,
		in:       time.Second,
		diffOpts: optsOf(jsonv1.FormatDurationAsNano),
	}, {
		mode: CallBothButReturnV1,
		in: struct {
			S []any
			M map[string]any
		}{},
		diffOpts: optsOf(jsonv2.FormatNilSliceAsNull, jsonv2.FormatNilMapAsNull),
	}, {
		mode:     CallV2ButUponErrorReturnV1,
		in:       jsontext.Value(`{"dupe":null,"dupe":null}`),
		diffOpts: optsOf(jsontext.AllowDuplicateNames),
	}, {
		mode: CallV1ButUponErrorReturnV2,
		in:   jsontext.Value(`{"dupe":null,"dupe":null}`),
	}, {
		mode:     CallBothButReturnV1,
		in:       big.Int{}, // MarshalJSON declared on pointer receiver
		diffOpts: optsOf(jsonv1.CallMethodsWithLegacySemantics, jsonv1.ReportErrorsWithLegacySemantics),
	}, {
		mode:     CallBothButReturnV2,
		in:       [32]byte{},
		diffOpts: optsOf(jsonv1.FormatByteArrayAsArray),
	}, {
		mode: CallBothButReturnV1,
		in: func() any {
			type namedByte byte
			return []namedByte{}
		}(),
		diffOpts: optsOf(jsonv1.FormatBytesWithLegacySemantics),
	}} {
		t.Run("", func(t *testing.T) {
			codec.SetMarshalCallMode(tt.mode)

			// Marshal via the codec, jsonv1, and jsonv2.
			c := callerPlus(caller(), 1)
			gotBuf, gotErr := codec.Marshal(tt.in, tt.inOpts)
			wantBufV1, wantErrV1 := jsonv1Marshal(tt.in, tt.inOpts)
			wantBufV2, wantErrV2 := jsonv2.Marshal(tt.in, tt.inOpts)
			hasDiff := !bytes.Equal(wantBufV1, wantBufV2) || !codec.errorsEqual(wantErrV1, wantErrV2)

			// Check the result.
			var wantBuf []byte
			var wantErr error
			switch tt.mode {
			case OnlyCallV1:
				wantMetrics.NumMarshalOnlyCallV1.Add(1)
				wantBuf, wantErr = wantBufV1, wantErrV1
				wantMetrics.NumMarshalReturnV1.Add(1)
			case CallV1ButUponErrorReturnV2:
				if wantErrV1 == nil {
					wantMetrics.NumMarshalOnlyCallV1.Add(1)
					wantBuf, wantErr = wantBufV1, wantErrV1
					wantMetrics.NumMarshalReturnV1.Add(1)
				} else {
					wantMetrics.NumMarshalCallBoth.Add(1)
					wantBuf, wantErr = wantBufV2, wantErrV2
					wantMetrics.NumMarshalReturnV2.Add(1)
				}
			case CallBothButReturnV1:
				wantMetrics.NumMarshalCallBoth.Add(1)
				wantBuf, wantErr = wantBufV1, wantErrV1
				wantMetrics.NumMarshalReturnV1.Add(1)
			case CallBothButReturnV2:
				wantMetrics.NumMarshalCallBoth.Add(1)
				wantBuf, wantErr = wantBufV2, wantErrV2
				wantMetrics.NumMarshalReturnV2.Add(1)
			case CallV2ButUponErrorReturnV1:
				if wantErrV2 == nil {
					wantMetrics.NumMarshalOnlyCallV2.Add(1)
					wantBuf, wantErr = wantBufV2, wantErrV2
					wantMetrics.NumMarshalReturnV2.Add(1)
				} else {
					wantMetrics.NumMarshalCallBoth.Add(1)
					wantBuf, wantErr = wantBufV1, wantErrV1
					wantMetrics.NumMarshalReturnV1.Add(1)
				}
			case OnlyCallV2:
				wantMetrics.NumMarshalOnlyCallV2.Add(1)
				wantBuf, wantErr = wantBufV2, wantErrV2
				wantMetrics.NumMarshalReturnV2.Add(1)
			}
			wantMetrics.NumMarshalTotal.Add(1)
			if gotErr != nil {
				wantMetrics.NumMarshalErrors.Add(1)
			}
			wantMetrics.MarshalSizeHistogram.insertSize(len(gotBuf))
			if !bytes.Equal(gotBuf, wantBuf) || !reflect.DeepEqual(gotErr, wantErr) {
				t.Errorf("Marshal:\n\tgot  (%s, %v)\n\twant (%s, %v)", gotBuf, gotErr, wantBuf, wantErr)
			}

			// Check any reported difference.
			var wantDiff Difference
			if (wantMetrics.NumMarshalCallBoth.Value() > 0 && hasDiff) || tt.diffOpts != nil {
				wantDiff = Difference{
					Caller: c, Func: "Marshal",
					GoType: reflect.TypeOf(tt.in), GoValue: tt.in,
					JSONValueV1: wantBufV1, JSONValueV2: wantBufV2,
					ErrorV1: wantErrV1, ErrorV2: wantErrV2,
					Options: jsonv2.JoinOptions(tt.diffOpts),
				}
			}
			if d := cmp.Diff(gotDiff, wantDiff,
				cmp.Comparer(func(x, y reflect.Type) bool { return x == y }),
				cmp.Comparer(func(x, y error) bool { return reflect.DeepEqual(x, y) }),
				cmp.Transformer("OptionNames", func(opts jsonv2.Options) []string {
					return slices.Collect(optionNames(opts))
				}),
				cmp.Exporter(func(t reflect.Type) bool { return true }),
			); d != "" {
				t.Errorf("Difference mismatch (-got +want):\n%s", d)
			}
			gotDiff = Difference{} // clear for next test run

			// Check metrics.
			codec.CodecMetrics.ExecTimeMarshalV1Nanos.Set(0)
			codec.CodecMetrics.ExecTimeMarshalV2Nanos.Set(0)
			if d := cmp.Diff(codec.CodecMetrics.ExpVar(), wantMetrics.ExpVar(),
				cmp.Transformer("UnmarshalJSON", func(in expvar.Var) (out any) {
					json.Unmarshal([]byte(in.String()), &out)
					return out
				}),
			); d != "" {
				t.Errorf("Metrics mismatch (-got +want):\n%s", d)
			}
			wantMetrics = CodecMetrics{}        // clear for next test run
			codec.CodecMetrics = CodecMetrics{} // clear for next test run
		})
	}
}

func TestCodecUnmarshal(t *testing.T) {
	var gotDiff Difference
	var wantMetrics CodecMetrics
	codec := Codec{
		AutoDetectOptions: true,
		ReportDifference: func(d Difference) {
			gotDiff = d
			wantMetrics.NumUnmarshalDiffs.Add(1)
			wantMetrics.UnmarshalCallerHistogram.Add(d.Caller, 1)
			for name := range optionNames(d.Options) {
				wantMetrics.UnmarshalOptionHistogram.Add(name, 1)
			}
		},
	}

	for _, tt := range []struct {
		mode     CallMode
		in       []byte
		newOut   func() any
		inOpts   jsonv2.Options
		diffOpts jsonv2.Options
		canClone bool
	}{{
		mode:   OnlyCallV1,
		in:     []byte("\"\xde\xad\xbe\xef\""),
		newOut: newer[string](),
	}, {
		mode:   CallV1ButUponErrorReturnV2,
		in:     []byte("\"\xde\xad\xbe\xef\""),
		newOut: newer[string](),
	}, {
		mode:     CallBothButReturnV1,
		in:       []byte("\"\xde\xad\xbe\xef\""),
		newOut:   newer[string](),
		diffOpts: optsOf(jsontext.AllowInvalidUTF8),
	}, {
		mode:   CallBothButReturnV1,
		in:     []byte("\"\xde\xad\xbe\xef\""),
		newOut: newer[string](),
		inOpts: jsontext.AllowInvalidUTF8(false),
	}, {
		mode:   CallBothButReturnV1,
		in:     []byte("\"\xde\xad\xbe\xef\""),
		newOut: newer[string](),
		inOpts: jsontext.AllowInvalidUTF8(true),
	}, {
		mode:   CallBothButReturnV2,
		in:     []byte("\"\xde\xad\xbe\xef\""),
		newOut: newer[string](),
		inOpts: jsontext.AllowInvalidUTF8(false),
	}, {
		mode:   CallBothButReturnV2,
		in:     []byte("\"\xde\xad\xbe\xef\""),
		newOut: newer[string](),
		inOpts: jsontext.AllowInvalidUTF8(true),
	}, {
		mode:     CallV2ButUponErrorReturnV1,
		in:       []byte("\"\xde\xad\xbe\xef\""),
		newOut:   newer[string](),
		diffOpts: optsOf(jsontext.AllowInvalidUTF8),
	}, {
		mode:   OnlyCallV2,
		in:     []byte("\"\xde\xad\xbe\xef\""),
		newOut: newer[string](),
	}, {
		mode:     CallV2ButUponErrorReturnV1,
		in:       []byte(`{"dupe":1,"dupe":2}`),
		newOut:   newer[map[string]int](),
		diffOpts: optsOf(jsontext.AllowDuplicateNames),
	}, {
		mode:   CallV2ButUponErrorReturnV1,
		in:     []byte(`{"dupe":1,"dupe":2}`),
		newOut: newer[map[string]int](),
		inOpts: jsontext.AllowDuplicateNames(true),
	}, {
		mode:   CallV2ButUponErrorReturnV1,
		in:     []byte(`{"firstname":"john","FIRSTNAME":"jim"}`),
		newOut: newer[struct{ FirstName string }](),
	}, {
		mode:     CallBothButReturnV2,
		in:       []byte(`{"firstname":"john","FIRSTNAME":"jim"}`),
		newOut:   newer[struct{ FirstName string }](),
		diffOpts: optsOf(jsontext.AllowDuplicateNames, jsonv2.MatchCaseInsensitiveNames),
	}, {
		mode:     CallV2ButUponErrorReturnV1,
		in:       []byte(`{"firstname":"john","FIRSTNAME":"jim"}`),
		newOut:   newer[struct{ FirstName string }](),
		inOpts:   jsonv2.MatchCaseInsensitiveNames(true),
		diffOpts: optsOf(jsontext.AllowDuplicateNames),
	}, {
		mode:     CallBothButReturnV2,
		in:       []byte(`{"first_name":"john","FIRST_NAME":"jim"}`),
		newOut:   newer[struct{ FirstName string }](),
		inOpts:   jsonv2.JoinOptions(jsonv2.MatchCaseInsensitiveNames(true)),
		diffOpts: optsOf(jsonv1.MatchCaseSensitiveDelimiter),
	}, {
		mode:     CallBothButReturnV2,
		in:       []byte(`{"first_name":"john","FIRST_NAME":"jim"}`),
		newOut:   newer[struct{ FirstName string }](),
		inOpts:   jsonv2.JoinOptions(jsonv2.MatchCaseInsensitiveNames(true), jsonv1.MatchCaseSensitiveDelimiter(false)),
		diffOpts: optsOf(jsontext.AllowDuplicateNames),
	}, {
		mode:     CallV1ButUponErrorReturnV2,
		in:       []byte(`"AAAAAAAAAAAAAAAAAAAAAA=="`),
		newOut:   newer[[16]byte](),
		diffOpts: optsOf(jsonv1.FormatByteArrayAsArray),
	}, {
		mode:     CallV2ButUponErrorReturnV1,
		in:       []byte(`"AAAA\r\nAAAAAAAAAAAAAAAAAA=="`),
		newOut:   newer[[]byte](),
		diffOpts: optsOf(jsonv1.ParseBytesWithLooseRFC4648),
	}, {
		mode:     CallBothButReturnV1,
		in:       []byte(`[1,2,3]`),
		newOut:   newer[[]byte](),
		diffOpts: optsOf(jsonv1.FormatBytesWithLegacySemantics),
	}, {
		mode:     CallV2ButUponErrorReturnV1,
		in:       []byte(`"2000-01-01T00:00:00,0Z"`),
		newOut:   newer[time.Time](),
		diffOpts: optsOf(jsonv1.ParseTimeWithLooseRFC3339),
	}, {
		mode:     CallV2ButUponErrorReturnV1,
		in:       []byte(`[1,2,3]`),
		newOut:   newer[[4]int](),
		diffOpts: optsOf(jsonv1.UnmarshalArrayFromAnyLength),
	}, {
		mode:     CallV2ButUponErrorReturnV1,
		in:       []byte(`1234`),
		newOut:   newer[time.Duration](),
		diffOpts: optsOf(jsonv1.FormatDurationAsNano),
	}, {
		mode: CallBothButReturnV2,
		in:   []byte(`{"A":"true"}`),
		newOut: newer[struct {
			A bool `json:",string"`
		}](),
		diffOpts: optsOf(jsonv1.StringifyWithLegacySemantics),
	}, {
		mode: CallBothButReturnV1,
		in:   []byte(`{"A":0.0}`),
		newOut: newer[struct {
			A float64 `json:",format:nonfinite,omitzero"`
		}](),
		diffOpts: optsOf(jsonv1.ReportErrorsWithLegacySemantics),
	}, {
		mode: CallBothButReturnV1,
		in:   []byte(`{"2000-01-01T00:00:00Z":5}`),
		newOut: newer[map[struct {
			time.Time
			encoding.TextUnmarshaler // cancels out UnmarshalText in time.Time, leaving only UnmarshalJSON
		}]int](),
		diffOpts: optsOf(jsonv1.CallMethodsWithLegacySemantics),
	}, {
		mode:   CallV1ButUponErrorReturnV2,
		in:     []byte(`{"Fizz":null}`),
		newOut: func() any { return &struct{ Fizz string }{"something"} },
	}, {
		mode:   CallBothButReturnV1,
		in:     []byte(`{"Fizz":null}`),
		newOut: func() any { return &struct{ Fizz string }{"something"} },
	}, {
		mode:   CallBothButReturnV2,
		in:     []byte(`{"Fizz":null}`),
		newOut: func() any { return &struct{ Fizz string }{"something"} },
	}, {
		mode:   CallV2ButUponErrorReturnV1,
		in:     []byte(`{"Fizz":null}`),
		newOut: func() any { return &struct{ Fizz string }{"something"} },
	}, {
		mode:     CallBothButReturnV2,
		in:       []byte(`{"Fizz":null}`),
		newOut:   func() any { return &struct{ Fizz string }{"something"} },
		canClone: true,
		diffOpts: optsOf(jsonv1.MergeWithLegacySemantics),
	}} {
		t.Run("", func(t *testing.T) {
			codec.SetUnmarshalCallMode(tt.mode)
			codec.CloneGoValue = nil
			if tt.canClone {
				codec.CloneGoValue = func(in any) any {
					out := tt.newOut()
					if !reflect.DeepEqual(in, out) {
						t.Error("clone is not equal")
					}
					return out
				}
			}

			// Unmarshal via the codec, jsonv1, and jsonv2.
			c := callerPlus(caller(), 2)
			gotVal, wantValV1, wantValV2 := tt.newOut(), tt.newOut(), tt.newOut()
			gotErr := codec.Unmarshal(tt.in, gotVal, tt.inOpts)
			wantErrV1 := jsonv1Unmarshal(tt.in, wantValV1, tt.inOpts)
			wantErrV2 := jsonv2.Unmarshal(tt.in, wantValV2, tt.inOpts)
			hasDiff := !reflect.DeepEqual(wantValV1, wantValV2) || !codec.errorsEqual(wantErrV1, wantErrV2)
			isMerge := !isPointerToZero(reflect.ValueOf(tt.newOut()))
			cantClone := codec.cloneGoValue(tt.newOut()) == nil

			// Check the result.
			var wantVal any
			var wantErr error
			switch tt.mode {
			case OnlyCallV1:
				wantMetrics.NumUnmarshalOnlyCallV1.Add(1)
				wantVal, wantErr = wantValV1, wantErrV1
				wantMetrics.NumUnmarshalReturnV1.Add(1)
			case CallV1ButUponErrorReturnV2:
				switch {
				case cantClone:
					wantMetrics.NumUnmarshalCallBothSkipped.Add(1)
					fallthrough
				case wantErrV1 == nil:
					wantMetrics.NumUnmarshalOnlyCallV1.Add(1)
					wantVal, wantErr = wantValV1, wantErrV1
					wantMetrics.NumUnmarshalReturnV1.Add(1)
				default:
					wantMetrics.NumUnmarshalCallBoth.Add(1)
					wantVal, wantErr = wantValV2, wantErrV2
					wantMetrics.NumUnmarshalReturnV2.Add(1)
				}
			case CallBothButReturnV1:
				if cantClone {
					wantMetrics.NumUnmarshalCallBothSkipped.Add(1)
					wantMetrics.NumUnmarshalOnlyCallV1.Add(1)
					wantVal, wantErr = wantValV1, wantErrV1
					wantMetrics.NumUnmarshalReturnV1.Add(1)
				} else {
					wantMetrics.NumUnmarshalCallBoth.Add(1)
					wantVal, wantErr = wantValV1, wantErrV1
					wantMetrics.NumUnmarshalReturnV1.Add(1)
				}
			case CallBothButReturnV2:
				if cantClone {
					wantMetrics.NumUnmarshalCallBothSkipped.Add(1)
					wantMetrics.NumUnmarshalOnlyCallV2.Add(1)
					wantVal, wantErr = wantValV2, wantErrV2
					wantMetrics.NumUnmarshalReturnV2.Add(1)
				} else {
					wantMetrics.NumUnmarshalCallBoth.Add(1)
					wantVal, wantErr = wantValV2, wantErrV2
					wantMetrics.NumUnmarshalReturnV2.Add(1)
				}
			case CallV2ButUponErrorReturnV1:
				switch {
				case cantClone:
					wantMetrics.NumUnmarshalCallBothSkipped.Add(1)
					fallthrough
				case wantErrV2 == nil:
					wantMetrics.NumUnmarshalOnlyCallV2.Add(1)
					wantVal, wantErr = wantValV2, wantErrV2
					wantMetrics.NumUnmarshalReturnV2.Add(1)
				default:
					wantMetrics.NumUnmarshalCallBoth.Add(1)
					wantVal, wantErr = wantValV1, wantErrV1
					wantMetrics.NumUnmarshalReturnV1.Add(1)
				}
			case OnlyCallV2:
				wantMetrics.NumUnmarshalOnlyCallV2.Add(1)
				wantVal, wantErr = wantValV2, wantErrV2
				wantMetrics.NumUnmarshalReturnV2.Add(1)
			}
			wantMetrics.NumUnmarshalTotal.Add(1)
			if isMerge {
				wantMetrics.NumUnmarshalMerge.Add(1)
			}
			if gotErr != nil {
				wantMetrics.NumUnmarshalErrors.Add(1)
			}
			wantMetrics.UnmarshalSizeHistogram.insertSize(len(tt.in))
			if !reflect.DeepEqual(gotVal, wantVal) || !reflect.DeepEqual(gotErr, wantErr) {
				t.Errorf("Unmarshal:\n\tgot  (%s, %v)\n\twant (%s, %v)", gotVal, gotErr, wantVal, wantErr)
			}

			// Check any reported difference.
			var wantDiff Difference
			if (wantMetrics.NumUnmarshalCallBoth.Value() > 0 && hasDiff) || tt.diffOpts != nil {
				wantDiff = Difference{
					Caller: c, Func: "Unmarshal",
					GoType: reflect.TypeOf(gotVal), JSONValue: tt.in,
					GoValueV1: wantValV1, GoValueV2: wantValV2,
					ErrorV1: wantErrV1, ErrorV2: wantErrV2,
					Options: jsonv2.JoinOptions(tt.diffOpts),
				}
			}
			if d := cmp.Diff(gotDiff, wantDiff,
				cmp.Comparer(func(x, y reflect.Type) bool { return x == y }),
				cmp.Comparer(func(x, y error) bool { return reflect.DeepEqual(x, y) }),
				cmp.Transformer("OptionNames", func(opts jsonv2.Options) []string {
					return slices.Collect(optionNames(opts))
				}),
				cmp.Exporter(func(t reflect.Type) bool { return true }),
			); d != "" {
				t.Errorf("Difference mismatch (-got +want):\n%s", d)
			}
			gotDiff = Difference{} // clear for next test run

			// Check metrics.
			codec.CodecMetrics.ExecTimeUnmarshalV1Nanos.Set(0)
			codec.CodecMetrics.ExecTimeUnmarshalV2Nanos.Set(0)
			if d := cmp.Diff(codec.CodecMetrics.ExpVar(), wantMetrics.ExpVar(),
				cmp.Transformer("UnmarshalJSON", func(in expvar.Var) (out any) {
					json.Unmarshal([]byte(in.String()), &out)
					return out
				}),
			); d != "" {
				t.Errorf("Metrics mismatch (-got +want):\n%s", d)
			}
			wantMetrics = CodecMetrics{}        // clear for next test run
			codec.CodecMetrics = CodecMetrics{} // clear for next test run
		})
	}
}

func TestCallModeRatio(t *testing.T) {
	for _, tt := range []struct {
		mode1 CallMode
		mode2 CallMode
		ratio float64
	}{
		{OnlyCallV1, OnlyCallV1, 0},
		{OnlyCallV1, OnlyCallV1, 1.0},
		{OnlyCallV1, CallBothButReturnV1, 0.3},
		{CallBothButReturnV1, CallBothButReturnV2, 0.7},
		{CallBothButReturnV2, OnlyCallV2, 0.8},
	} {
		var r callModeRatio
		r.storeModeRatio(tt.mode1, tt.mode2, tt.ratio)
		var n1, n2 int
		var ok bool
		for i := range 1_000_000 {
			m := r.loadRandomMode()
			if m != tt.mode1 && m != tt.mode2 {
				t.Errorf("got mode %v, want either mode %v or %v,", m, tt.mode1, tt.mode2)
			}
			if m == tt.mode1 {
				n1++
			}
			if m == tt.mode2 {
				n2++
			}
			if i++; i >= 1000 && math.Round(math.Log10(float64(i))) == math.Log10(float64(i)) {
				if tt.mode1 == tt.mode2 {
					ok = true
					break
				}
				ratio := float64(n2) / float64(n1+n2)
				if 0.99*tt.ratio <= ratio && ratio <= 1.01*tt.ratio {
					ok = true
					break
				}
			}
		}
		if !ok {
			ratio := float64(n2) / float64(n1+n2)
			t.Errorf("got ratio %0.3f, want ratio %0.3f", ratio, tt.ratio)
		}
	}

	if len(callModeNames) != int(maxCallMode) {
		t.Errorf("len(callModeNames) = %v, want %v", len(callModeNames), maxCallMode)
	}
}

func TestSizeHistogram(t *testing.T) {
	var h SizeHistogram
	for _, n := range []int{0, 1, 1, 4, 4, 15, 15, 16, 1050, 1000000, 2000000, 2000000, 2000000, 1e9, 1e12} {
		h.insertSize(n)
	}
	got := h.String()
	want := `{"<1B":1,"<2B":2,"<8B":2,"<16B":2,"<32B":1,"<2KiB":1,"<1MiB":1,"<2MiB":3,"<1GiB":1,"<1TiB":1}`
	var gotAny, wantAny any
	if err := json.Unmarshal([]byte(got), &gotAny); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(want), &wantAny); err != nil {
		t.Fatal(err)
	}
	if d := cmp.Diff(gotAny, wantAny); d != "" {
		t.Fatalf("mismatch (-got +want):\n%s", d)
	}
}

// Test that our copy of v1 options is in sync with the jsonv1 package.
func TestDefaultOptionsV1(t *testing.T) {
	var opts []jsonv2.Options
	for _, opt := range defaultOptionsV1 {
		opts = append(opts, opt(true))
	}
	got := jsonv2.JoinOptions(opts...)
	want := jsonv1.DefaultOptionsV1()

	if d := cmp.Diff(got, want,
		cmp.Exporter(func(reflect.Type) bool {
			return true
		}),
		cmp.FilterPath(func(p cmp.Path) bool {
			// Ignore presence since [jsonv1.DefaultOptionsV1]
			// explicitly sets irrelevant options to false.
			return p.String() == "Flags.Presence"
		}, cmp.Ignore()),
	); d != "" {
		t.Errorf("DefaultOptionsV1 mismatch (-got, +want):\n%s", d)
	}
}

func BenchmarkMarshal(b *testing.B) {
	var c Codec
	in := true
	for m := range callModeNames {
		b.Run(m.String(), func(b *testing.B) {
			c.SetMarshalCallMode(m)
			b.ReportAllocs()
			for range b.N {
				c.Marshal(&in)
			}
		})
	}
	b.Run("DirectV1", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			jsonv1.Marshal(&in)
		}
	})
	b.Run("DirectV2", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			jsonv2.Marshal(&in)
		}
	})
}

func BenchmarkUnmarshal(b *testing.B) {
	var c Codec
	in := []byte("true")
	var out bool
	for m := range callModeNames {
		b.Run(m.String(), func(b *testing.B) {
			c.SetMarshalCallMode(m)
			b.ReportAllocs()
			for range b.N {
				c.Unmarshal(in, &out)
			}
		})
	}
	b.Run("DirectV1", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			jsonv1.Unmarshal(in, &out)
		}
	})
	b.Run("DirectV2", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			jsonv2.Unmarshal(in, &out)
		}
	})
}

func BenchmarkAutoDetectOptions(b *testing.B) {
	in := []byte(`{"FIRSTNAME":"John","LASTNAME":"Doe","lastName":"Dupe"}`)
	type User struct {
		FirstName string   `json:"firstName"`
		LastName  string   `json:"lastName"`
		Age       int      `json:"age,omitempty"`
		Aliases   []string `json:"tags"`
	}

	for _, autoDetect := range []bool{false, true} {
		b.Run(fmt.Sprintf("AutoDetectOptions:%v", autoDetect), func(b *testing.B) {
			c := Codec{AutoDetectOptions: autoDetect}
			c.SetMarshalCallMode(CallBothButReturnV1)
			c.SetUnmarshalCallMode(CallBothButReturnV1)

			b.Run("Marshal", func(b *testing.B) {
				b.ReportAllocs()
				u := User{FirstName: "John", LastName: "Doe"}
				for range b.N {
					c.Marshal(&u)
				}
			})

			b.Run("Unmarshal", func(b *testing.B) {
				b.ReportAllocs()
				var u User
				for range b.N {
					u = User{}
					c.Unmarshal(in, &u)
				}
			})
		})
	}
}
