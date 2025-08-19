// Package jsonsplit provides JSON functionality that can dynamically switch
// the underlying implementation between [jsonv1] and [jsonv2].
// The purpose of this package is to provide a gradual means for migrating
// from v1 to v2 and detecting which options (if any) need to be specified
// in order to maintain backwards compatibility.
//
// Whether it is safe to directly use v2 is dependent on both
// static properties of the Go types being serialized and also
// dynamic properties of the input JSON text that is being unmarshaled.
// Many of the changed behaviors in v2 can be directly specified
// on Go struct fields to ensure that the struct type is represented
// in the same way in both v1 and v2. Alternatively, some options may
// need to be specified when calling [jsonv2.Marshal] or [jsonv2.Unmarshal]
// to preserve a particular v1 behavior.
//
// # Example usage and migration
//
// 1. Replace existing calls of [jsonv1.Unmarshal] with [jsonsplit.Unmarshal].
// By default, [jsonsplit] calls [jsonv1], so this is identical behavior.
//
// 2. Configure [jsonsplit] to call both v1 and v2 and report any differences:
//
//	func init() {
//		// Specify that when a difference is detected,
//		// to auto-detect which options are causing the difference.
//		jsonsplit.GlobalCodec.AutoDetectOptions = true
//
//		// Log every time we detect a difference between v1 and v2.
//		jsonsplit.GlobalCodec.ReportDifference(func(d jsonsplit.Difference) {
//			slog.Warn("detected jsonv1-to-jsonv2 difference", "diff", d)
//		})
//
//		// Specify that we try both v1 and v2 with some probability,
//		// but to always return v1 results.
//		jsonsplit.GlobalCodec.SetMarshalCallRatio(
//			jsonsplit.OnlyCallV1,          // 90% of the time
//			jsonsplit.CallBothButReturnV1, // 10% of the time
//			0.1,
//		)
//
//		// Publish an expvar under the "jsonsplit" name.
//		jsonsplit.Publish()
//	}
//
// While we can detect differences in behavior between v1 and v2,
// the semantic behavior is still identical to v1
// since both call modes are configured to return the v1 result.
//
// 3. Run the program and monitor logs and metrics.
// Let's suppose that through logging, we discover for this Go type:
//
//	type User struct {
//		FirstName string `json:"firstName"`
//		LastName  string `json:"lastName"`
//	}
//
// that unmarshal is being provided a JSON input like:
//
//	{
//		"FIRSTNAME": "John",
//		"LASTNAME":  "Doe"
//	}
//
// which happens to work fine in v1 because
// [jsonv1] uses case-insensitive matching by default, while
// [jsonv2] use case-sensitive matching by default.
//
// There are two ways to resolve this difference.
//
// 4a. (Option 1) We can mark every Go struct field as being case insensitive:
//
//	type User struct {
//		FirstName string `json:"firstName,case:ignore"`
//		LastName  string `json:"lastName,case:ignore"`
//	}
//
// This has the advantage of making sure this struct operates the same
// regardless of whether it is called by [jsonv1] or [jsonv2].
// However, this has the disadvantage of requiring tedious modification
// of every field in the Go struct and may not even be possible
// if the declaration of the Go type is not within your control.
//
// 4b. (Option 2) We can call [jsonv2] with [jsonv2.MatchCaseInsensitiveNames]:
//
//	... := jsonsplit.Marshal(v, jsonv2.MatchCaseInsensitiveNames(true))
//
// This has the advantage of being able alter the behavior of unmarshal
// at the call site, affecting all types that it recursively reaches.
// This has the disadvantage that the type will behave differently
// depending on whether it is called by [jsonv1] and [jsonv2]
// with the default behavior.
//
// 5. Let the program run for a while and
// gradually increase the ratio of trying both v1 and v2.
// If we detect no more differences, then we have decent confidence
// that we have handled all the relevant differences in behavior
// between v1 and v2. We can now gradually switch to using v2 exclusively:
//
//	func init() {
//		// Specify that we start to return v2 results with some probability.
//		jsonsplit.GlobalCodec.SetMarshalCallRatio(
//			jsonsplit.CallBothButReturnV1, // 90% of the time
//			jsonsplit.OnlyCallV2,          // 10% of the time
//			0.1,
//		)
//	}
//
// This will occasionally return the results of v2 and
// you can verify that your program continues to function as expected.
//
// 6. After increasing exclusive use of v2 to 100% and
// still not encountering any issues, we can now confidently replace
// [jsonsplit.Unmarshal] with [jsonv2.Unmarshal] (and possibly with
// [jsonv2.MatchCaseInsensitiveNames] if we need to maintain backwards
// compatibility or drop it if we decide to allow a breaking change).
package jsonsplit

import (
	"bytes"
	"expvar"
	"fmt"
	"iter"
	"maps"
	"math"
	"math/bits"
	"math/rand/v2"
	"reflect"
	"runtime"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"unicode"

	jsonv1std "encoding/json"

	jsonv2 "github.com/go-json-experiment/json"            // TODO: Use "encoding/json/v2"
	jsontext "github.com/go-json-experiment/json/jsontext" // TODO: Use "encoding/json/jsontext"
	jsonv1 "github.com/go-json-experiment/json/v1"         // TODO: Use "encoding/json"
)

var currentFile = func() string {
	_, file, _, _ := runtime.Caller(0)
	return file
}()

// caller determines the first caller outside of this source file.
func caller() string {
	const maxLocalFramesToIgnore = 10
	for i := range maxLocalFramesToIgnore {
		switch _, file, line, ok := runtime.Caller(i + 1); {
		case file == currentFile:
			continue
		case ok:
			return fmt.Sprintf("%s:%d", file, line)
		}
	}
	return ""
}

// GlobalCodec is a global instantiation of [Codec].
var GlobalCodec Codec

// Marshal marshals from v with either [jsonv1.Marshal] or [jsonv2.Marshal]
// depending on the mode specified in [Codec.SetMarshalCallRatio]
// on the [GlobalCodec] variable.
func Marshal(v any, o ...jsonv2.Options) (b []byte, err error) {
	return GlobalCodec.Marshal(v, o...)
}

// Unmarshal unmarshals into v with either [jsonv1.Unmarshal] or [jsonv2.Unmarshal]
// depending on the mode specified in [Codec.SetUnmarshalCallRatio]
// on the [GlobalCodec] variable.
func Unmarshal(b []byte, v any, o ...jsonv2.Options) error {
	return GlobalCodec.Unmarshal(b, v, o...)
}

// Publish calls [expvar.Publish] with [CodecMetrics.ExpVar] under the name "jsonsplit".
func Publish() {
	expvar.Publish("jsonsplit", GlobalCodec.ExpVar())
}

// Codec configures how to execute marshal and unmarshal calls.
// The exported fields must be set before concurrent use.
// The zero value is ready for use and by default will [OnlyCallV1].
type Codec struct {
	// AutoDetectOptions specifies whether to automatically detect which
	// [jsontext], [jsonv1], or [jsonv2] options are needed to preserve
	// identical behavior between v1 and v2 once a difference has been detected.
	//
	// Auto-detection is relatively slow and will need to run marshal/unmarshal
	// many extra times. In performance sensitive systems,
	// configure [Codec.SetMarshalCallRatio] and [Codec.SetUnmarshalCallRatio]
	// such that [CallBothButReturnV1] or [CallBothButReturnV2] call modes
	// occur with relatively low probability.
	AutoDetectOptions bool

	// ReportDifference is a custom function to report detected differences
	// in marshal or unmarshal. If nil, structured differences are ignored.
	// The fields in [Difference] alias the call arguments for marshal/unmarshal
	// and should therefore avoid leaking beyond the function call.
	// Must be set before any [Codec.Marshal] or [Codec.Unmarshal] calls.
	ReportDifference func(Difference)

	// EqualJSONValues is a custom function to compare JSON values after marshal.
	// If nil, it uses [bytes.Equal].
	EqualJSONValues func(jsontext.Value, jsontext.Value) bool

	// EqualGoValues is a custom function to compare Go values after unmarshal.
	// If nil, it uses [reflect.DeepEqual].
	EqualGoValues func(any, any) bool

	// EqualErrors is a custom function to compare errors from marshal or unmarshal.
	// If nil, it only checks whether the errors are both non-nil or both nil.
	EqualErrors func(error, error) bool

	// CloneGoValue is a custom function to deeply clone an arbitrary Go value
	// for use as the output for calling unmarshal.
	// If nil (or the function returns nil), then it clones any
	// pointers to a zero'd value by simply allocating a new one.
	CloneGoValue func(v any) any

	marshalCallRatio   callModeRatio
	unmarshalCallRatio callModeRatio

	CodecMetrics
}

// CodecMetrics contains metrics about marshal and unmarshal calls.
type CodecMetrics struct {
	// NumMarshalTotal is the total number of [Codec.Marshal] calls.
	NumMarshalTotal expvar.Int
	// NumMarshalErrors is the total number of [Codec.Marshal] calls
	// that returned an error.
	NumMarshalErrors expvar.Int
	// NumMarshalOnlyCallV1 is the number of [Codec.Marshal] calls
	// that only delegated the call to [jsonv1.Marshal].
	NumMarshalOnlyCallV1 expvar.Int
	// NumMarshalOnlyCallV2 is the number of [Codec.Marshal] calls
	// that only delegated the call to [jsonv2.Marshal].
	NumMarshalOnlyCallV2 expvar.Int
	// NumMarshalCallBoth is the number of [Codec.Marshal] calls
	// that called both [jsonv1.Marshal] and [jsonv2.Marshal].
	NumMarshalCallBoth expvar.Int
	// NumMarshalReturnV1 is the number of [Codec.Marshal] calls
	// that used the result of [jsonv1.Marshal].
	NumMarshalReturnV1 expvar.Int
	// NumMarshalReturnV2 is the number of [Codec.Marshal] calls
	// that used the result of [jsonv2.Marshal].
	NumMarshalReturnV2 expvar.Int
	// NumMarshalDiffs is the number of times that [Codec.Marshal] detected
	// a difference between the outputs of [jsonv1.Marshal] and [jsonv2.Marshal].
	NumMarshalDiffs expvar.Int

	// ExecTimeMarshalV1Nanos is the total number of nanoseconds
	// spent in a [jsonv1.Marshal] call when comparing both v1 and v2.
	// It excludes time spent only calling v1.
	ExecTimeMarshalV1Nanos expvar.Int
	// ExecTimeMarshalV2Nanos is the total number of nanoseconds
	// spent in a [jsonv2.Marshal] call when comparing both v1 and v2.
	// It excludes time spent only calling v2.
	ExecTimeMarshalV2Nanos expvar.Int

	// MarshalSizeHistogram is a histogram of JSON input sizes from [Codec.Marshal]
	// regardless of whether a difference is detected.
	MarshalSizeHistogram SizeHistogram
	// MarshalCallerHistogram is a histogram of callers to [Codec.Marshal]
	// whenever a difference is detected.
	MarshalCallerHistogram expvar.Map
	// MarshalOptionHistogram is a histogram of JSON options
	// that could be specified to [Codec.Marshal] to avoid a difference.
	MarshalOptionHistogram expvar.Map

	// NumUnmarshalTotal is the total number of [Codec.Unmarshal] calls.
	NumUnmarshalTotal expvar.Int
	// NumUnmarshalErrors is the total number of [Codec.Unmarshal] calls
	// that returned an error.
	NumUnmarshalErrors expvar.Int
	// NumUnmarshalMerge is the total number of [Codec.Unmarshal] calls
	// where the output argument is a pointer to a non-zero value.
	NumUnmarshalMerge expvar.Int
	// NumUnmarshalOnlyCallV1 is the number of [Codec.Unmarshal] calls
	// that only delegated the call to [jsonv1.Unmarshal].
	NumUnmarshalOnlyCallV1 expvar.Int
	// NumUnmarshalOnlyCallV2 is the number of [Codec.Unmarshal] calls
	// that only delegated the call to [jsonv2.Unmarshal].
	NumUnmarshalOnlyCallV2 expvar.Int
	// NumUnmarshalCallBoth is the number of [Codec.Unmarshal] calls
	// that called both [jsonv1.Unmarshal] and [jsonv2.Unmarshal].
	NumUnmarshalCallBoth expvar.Int
	// NumUnmarshalCallBothSkipped is the number of [Codec.Unmarshal] calls
	// that could not call both v1 and v2 because of some problem.
	NumUnmarshalCallBothSkipped expvar.Int
	// NumUnmarshalReturnV1 is the number of [Codec.Unmarshal] calls
	// that used the result of [jsonv1.Unmarshal].
	NumUnmarshalReturnV1 expvar.Int
	// NumUnmarshalReturnV2 is the number of [Codec.Unmarshal] calls
	// that used the result of [jsonv2.Unmarshal].
	NumUnmarshalReturnV2 expvar.Int
	// NumUnmarshalDiffs is the number of times that [Codec.Unmarshal] detected
	// a difference between the outputs of [jsonv1.Unmarshal] and [jsonv2.Unmarshal].
	NumUnmarshalDiffs expvar.Int

	// ExecTimeUnmarshalV1Nanos is the total number of nanoseconds
	// spent in a [jsonv1.Unmarshal] call when comparing both v1 and v2.
	ExecTimeUnmarshalV1Nanos expvar.Int
	// ExecTimeUnmarshalV2Nanos is the total number of nanoseconds
	// spent in a [jsonv2.Unmarshal] call when comparing both v1 and v2.
	ExecTimeUnmarshalV2Nanos expvar.Int

	// UnmarshalSizeHistogram is a histogram of JSON input sizes to [Codec.Unmarshal]
	// regardless of whether a difference is detected.
	UnmarshalSizeHistogram SizeHistogram
	// UnmarshalCallerHistogram is a histogram of callers to [Codec.Unmarshal]
	// whenever a difference is detected.
	UnmarshalCallerHistogram expvar.Map
	// UnmarshalOptionHistogram is a histogram of JSON options
	// that could be specified to [Codec.Unmarshal] to avoid a difference.
	UnmarshalOptionHistogram expvar.Map
}

// Difference is a structured representation of the difference detected
// between the outputs of a v1 and v2 marshal or unmarshal call.
type Difference struct {
	// Caller is the file and line number of the caller.
	Caller string `json:",omitzero"`
	// Func is the operation and is either "Marshal" or "Unmarshal".
	Func string `json:",omitzero"`
	// GoType is the Go type being operated upon.
	GoType reflect.Type `json:",omitzero"`

	// JSONValue is the input JSON value provided to an unmarshal call.
	JSONValue jsontext.Value `json:",omitzero"`
	// JSONValueV1 is the output JSON value produced by a v1 marshal call.
	JSONValueV1 jsontext.Value `json:",omitzero"`
	// JSONValueV2 is the output JSON value produced by a v2 marshal call.
	JSONValueV2 jsontext.Value `json:",omitzero"`

	// GoValue is the input Go value provided to a marshal call.
	GoValue any `json:"-"`
	// GoValueV1 is the output Go value populated by a v1 unmarshal call.
	GoValueV1 any `json:"-"`
	// GoValueV2 is the output Go value populated by a v2 unmarshal call.
	GoValueV2 any `json:"-"`

	// ErrorV1 is the error produced by a v1 marshal/unmarshal call.
	ErrorV1 error `json:",omitzero"`
	// ErrorV2 is the error produced by a v2 marshal/unmarshal call.
	ErrorV2 error `json:",omitzero"`

	// Options is the set of options that need to be enabled
	// in order to resolve any behavior difference between v1 and v2.
	// It is only populated if [Codec.AutoDetectOptions] is enabled.
	Options jsonv2.Options `json:",omitzero"`
}

var differenceOptions = sync.OnceValue(func() jsonv2.Options {
	return jsonv2.JoinOptions(
		jsontext.AllowDuplicateNames(true),
		jsontext.AllowInvalidUTF8(true),
		jsonv2.WithMarshalers(jsonv2.JoinMarshalers(
			jsonv2.MarshalToFunc(func(e *jsontext.Encoder, t reflect.Type) error {
				return e.WriteToken(jsontext.String(t.String()))
			}),
			jsonv2.MarshalToFunc(func(e *jsontext.Encoder, v jsontext.Value) error {
				if !v.IsValid(jsontext.AllowDuplicateNames(true), jsontext.AllowInvalidUTF8(true)) {
					// Best-effort preservation of invalid JSON input.
					v, _ = jsontext.AppendQuote(nil, "INVALID: "+string(v))
				}
				return e.WriteValue(v)
			}),
			jsonv2.MarshalToFunc(func(e *jsontext.Encoder, err error) error {
				return e.WriteToken(jsontext.String(err.Error()))
			}),
			jsonv2.MarshalToFunc(func(e *jsontext.Encoder, opts jsonv2.Options) error {
				return jsonv2.MarshalEncode(e, slices.Collect(optionNames(opts)))
			}),
		)),
	)
})

// MarshalJSON marshals d as JSON in a non-reversible manner and
// is primarily intended for logging purposes.
//
// In particular, it uses:
//   - [reflect.Type.String] to encode a Go type
//   - [error.Error] to encode a Go error
//   - [Difference.OptionNames] to encode a [jsonv2.Options]
func (d Difference) MarshalJSON() ([]byte, error) {
	type difference Difference
	return jsonv2.Marshal(difference(d), differenceOptions())
}

// String returns the difference as JSON.
func (d Difference) String() string {
	b, _ := d.MarshalJSON()
	return string(b)
}

// OptionNames returns an iterator over the names of all the enabled options in
// [Difference.Options] that resolve any behavior difference between v1 and v2.
func (d Difference) OptionNames() iter.Seq[string] {
	return optionNames(d.Options)
}

// sortedOptionNames is list a sorted list of all options that
// define behavior differences between v1 and v2.
var sortedOptionNames = sync.OnceValue(func() []string {
	names := slices.Collect(maps.Keys(defaultOptionsV1))
	slices.Sort(names)
	return names
})

func optionNames(opts jsonv2.Options) iter.Seq[string] {
	return func(yield func(string) bool) {
		for _, name := range sortedOptionNames() {
			if v, ok := jsonv2.GetOption(opts, defaultOptionsV1[name]); v && ok {
				if !yield(name) {
					return
				}
			}
		}
	}
}

// CallMode configures how [Codec.Marshal] and [Codec.Unmarshal]
// delegates calls to either v1 or v2 functionality.
type CallMode int

const (
	// OnlyCallV1 specifies to only call v1 functionality.
	OnlyCallV1 CallMode = iota
	// CallV1ButUponErrorReturnV2 specifies to call v1 by default,
	// but only when an error occurs, to call v2 and return its result instead.
	CallV1ButUponErrorReturnV2
	// CallBothButReturnV1 specifies to call both v1 and v2 functionality,
	// but to return the results for v1.
	CallBothButReturnV1
	// CallBothButReturnV2 specifies to call both v1 and v2 functionality,
	// but to return the results for v2.
	CallBothButReturnV2
	// CallV2ButUponErrorReturnV1 specifies to call v2 by default,
	// but only when an error occurs, to call v1 and return its result instead.
	CallV2ButUponErrorReturnV1
	// OnlyCallV2 specifies to only call v2 functionality.
	OnlyCallV2

	maxCallMode
)

var callModeNames = map[CallMode]string{
	OnlyCallV1:                 "OnlyCallV1",
	CallV1ButUponErrorReturnV2: "CallV1ButUponErrorReturnV2",
	CallBothButReturnV1:        "CallBothButReturnV1",
	CallBothButReturnV2:        "CallBothButReturnV2",
	CallV2ButUponErrorReturnV1: "CallV2ButUponErrorReturnV1",
	OnlyCallV2:                 "OnlyCallV2",
}

func (m CallMode) String() string {
	if name, ok := callModeNames[m]; ok {
		return name
	}
	return fmt.Sprintf("CallMode(%d)", m)
}

func (m CallMode) checkValid() {
	if m < 0 || m >= maxCallMode {
		panic("invalid mode")
	}
}

// Marshal marshals from v with either [jsonv1.Marshal] or [jsonv2.Marshal]
// depending on the mode specified in [Codec.SetMarshalCallRatio].
// If both v1 and v2 are called, it checks whether any differences
// are detected in the serialized JSON output values.
//
// The specified options o is applied on top of the default v1 or v2 options.
// If o is empty or is exactly equal to [jsonv1.DefaultOptionsV1],
// then this calls [jsonv1std.Marshal] instead of [jsonv1.Marshal]
// when operating in v1 mode. This allows for detection of differences
// between [jsonv1std] and [jsonv1].
func (c *Codec) Marshal(v any, o ...jsonv2.Options) (b []byte, err error) {
	c.NumMarshalTotal.Add(1)
	defer func() {
		c.MarshalSizeHistogram.insertSize(len(b))
		if err != nil {
			c.NumMarshalErrors.Add(1)
		}
	}()

	switch mode := c.marshalCallRatio.loadRandomMode(); mode {
	case OnlyCallV1:
		c.NumMarshalOnlyCallV1.Add(1)
		c.NumMarshalReturnV1.Add(1)
		return jsonv1Marshal(v, o...)
	case OnlyCallV2:
		c.NumMarshalOnlyCallV2.Add(1)
		c.NumMarshalReturnV2.Add(1)
		return jsonv2.Marshal(v, o...)
	case CallV1ButUponErrorReturnV2, CallBothButReturnV1, CallBothButReturnV2, CallV2ButUponErrorReturnV1:
		// Marshal both through v1 and v2 and verify results are identical.
		var buf1, buf2 []byte
		var err1, err2 error
		var dur1, dur2 time.Duration
		switch mode {
		case CallV1ButUponErrorReturnV2:
			dur1 = elapsed(func() { buf1, err1 = jsonv1Marshal(v, o...) })
			if err1 == nil {
				c.NumMarshalOnlyCallV1.Add(1)
				c.NumMarshalReturnV1.Add(1)
				return buf1, nil
			}
			dur2 = elapsed(func() { buf2, err2 = jsonv2.Marshal(v, o...) })
		case CallV2ButUponErrorReturnV1:
			dur2 = elapsed(func() { buf2, err2 = jsonv2.Marshal(v, o...) })
			if err2 == nil {
				c.NumMarshalOnlyCallV2.Add(1)
				c.NumMarshalReturnV2.Add(1)
				return buf2, nil
			}
			dur1 = elapsed(func() { buf1, err1 = jsonv1Marshal(v, o...) })
		case CallBothButReturnV1, CallBothButReturnV2:
			dur1 = elapsed(func() { buf1, err1 = jsonv1Marshal(v, o...) })
			dur2 = elapsed(func() { buf2, err2 = jsonv2.Marshal(v, o...) })
		}
		c.NumMarshalCallBoth.Add(1)
		c.ExecTimeMarshalV1Nanos.Add(int64(dur1))
		c.ExecTimeMarshalV2Nanos.Add(int64(dur2))

		// Check for differences.
		if !(c.jsonEqual(buf1, buf2) && c.errorsEqual(err1, err2)) {
			caller := caller()
			c.NumMarshalDiffs.Add(1)
			c.MarshalCallerHistogram.Add(caller, 1)

			var options jsonv2.Options
			if c.AutoDetectOptions {
				options = autoDetectOptions(func(o ...jsonv2.Options) bool {
					buf2, err2 := jsonv2.Marshal(v, o...)
					return c.jsonEqual(buf1, buf2) && c.errorsEqual(err1, err2)
				}, o...)
				for name := range optionNames(options) {
					c.MarshalOptionHistogram.Add(name, 1)
				}
			}

			if c.ReportDifference != nil {
				c.ReportDifference(Difference{
					Caller:      caller,
					Func:        "Marshal",
					GoType:      reflect.TypeOf(v),
					GoValue:     v,
					JSONValueV1: buf1,
					JSONValueV2: buf2,
					ErrorV1:     err1,
					ErrorV2:     err2,
					Options:     options,
				})
			}
		}

		// Select the appropriate return value.
		switch mode {
		case CallBothButReturnV1, CallV2ButUponErrorReturnV1:
			c.NumMarshalReturnV1.Add(1)
			return buf1, err1
		case CallBothButReturnV2, CallV1ButUponErrorReturnV2:
			c.NumMarshalReturnV2.Add(1)
			return buf2, err2
		}
	}
	panic("unknown mode")
}

// Unmarshal unmarshals to v with either [jsonv1.Unmarshal] or [jsonv2.Unmarshal]
// depending on the mode specified in [Codec.SetUnmarshalCallRatio].
// If both v1 and v2 are called, it checks whether any differences
// are detected in the deserialized Go output values.
//
// The specified options o is applied on top of the default v1 or v2 options.
// If o is empty or is exactly equal to [jsonv1.DefaultOptionsV1],
// then this calls [jsonv1std.Unmarshal] instead of [jsonv1.Unmarshal]
// when operating in v1 mode. This allows for detection of differences
// between [jsonv1std] and [jsonv1].
func (c *Codec) Unmarshal(b []byte, v any, o ...jsonv2.Options) (err error) {
	c.NumUnmarshalTotal.Add(1)
	c.UnmarshalSizeHistogram.insertSize(len(b))
	if !isPointerToZero(reflect.ValueOf(v)) {
		c.NumUnmarshalMerge.Add(1)
	}
	defer func() {
		if err != nil {
			c.NumUnmarshalErrors.Add(1)
		}
	}()

	switch mode := c.unmarshalCallRatio.loadRandomMode(); mode {
	case OnlyCallV1:
		c.NumUnmarshalOnlyCallV1.Add(1)
		c.NumUnmarshalReturnV1.Add(1)
		return jsonv1Unmarshal(b, v, o...)
	case OnlyCallV2:
		c.NumUnmarshalOnlyCallV2.Add(1)
		c.NumUnmarshalReturnV2.Add(1)
		return jsonv2.Unmarshal(b, v, o...)
	case CallV1ButUponErrorReturnV2, CallBothButReturnV1, CallBothButReturnV2, CallV2ButUponErrorReturnV1:
		// Make sure we can clone the output, otherwise we cannot call both.
		valOrig := c.cloneGoValue(v)
		if valOrig == nil {
			c.NumUnmarshalCallBothSkipped.Add(1)
			switch mode {
			case CallV1ButUponErrorReturnV2, CallBothButReturnV1:
				c.NumUnmarshalOnlyCallV1.Add(1)
				c.NumUnmarshalReturnV1.Add(1)
				return jsonv1Unmarshal(b, v, o...)
			case CallBothButReturnV2, CallV2ButUponErrorReturnV1:
				c.NumUnmarshalOnlyCallV2.Add(1)
				c.NumUnmarshalReturnV2.Add(1)
				return jsonv2.Unmarshal(b, v, o...)
			}
		}

		// Unmarshal both through v1 and v2 and verify results are identical.
		var val1, val2 any
		var err1, err2 error
		var dur1, dur2 time.Duration
		switch mode {
		case CallV1ButUponErrorReturnV2:
			val1 = v
			dur1 = elapsed(func() { err1 = jsonv1Unmarshal(b, val1, o...) })
			if err1 == nil {
				c.NumUnmarshalOnlyCallV1.Add(1)
				c.NumUnmarshalReturnV1.Add(1)
				return nil
			}
			val2 = c.cloneGoValue(valOrig)
			dur2 = elapsed(func() { err2 = jsonv2.Unmarshal(b, val2, o...) })
			val1 = shallowCopy(v, val2) // v has v1 results, but needs v2
		case CallV2ButUponErrorReturnV1:
			val2 = v
			dur2 = elapsed(func() { err2 = jsonv2.Unmarshal(b, val2, o...) })
			if err2 == nil {
				c.NumUnmarshalOnlyCallV2.Add(1)
				c.NumUnmarshalReturnV2.Add(1)
				return nil
			}
			val1 = c.cloneGoValue(valOrig)
			dur1 = elapsed(func() { err1 = jsonv1Unmarshal(b, val1, o...) })
			val2 = shallowCopy(v, val1) // v has v2 results, but needs v1
		case CallBothButReturnV1:
			val1 = v
			dur1 = elapsed(func() { err1 = jsonv1Unmarshal(b, val1, o...) })
			val2 = c.cloneGoValue(valOrig)
			dur2 = elapsed(func() { err2 = jsonv2.Unmarshal(b, val2, o...) })
		case CallBothButReturnV2:
			val1 = c.cloneGoValue(valOrig)
			dur1 = elapsed(func() { err1 = jsonv1Unmarshal(b, val1, o...) })
			val2 = v
			dur2 = elapsed(func() { err2 = jsonv2.Unmarshal(b, val2, o...) })
		}
		c.NumUnmarshalCallBoth.Add(1)
		c.ExecTimeUnmarshalV1Nanos.Add(int64(dur1))
		c.ExecTimeUnmarshalV2Nanos.Add(int64(dur2))

		// Check for differences.
		if !(c.goEqual(val1, val2) && c.errorsEqual(err1, err2)) {
			caller := caller()
			c.NumUnmarshalDiffs.Add(1)
			c.UnmarshalCallerHistogram.Add(caller, 1)

			var options jsonv2.Options
			if c.AutoDetectOptions {
				options = autoDetectOptions(func(o ...jsonv2.Options) bool {
					val2 := c.cloneGoValue(valOrig)
					err2 := jsonv2.Unmarshal(b, val2, o...)
					return c.goEqual(val1, val2) && c.errorsEqual(err1, err2)
				}, o...)
				for name := range optionNames(options) {
					c.UnmarshalOptionHistogram.Add(name, 1)
				}
			}

			if c.ReportDifference != nil {
				c.ReportDifference(Difference{
					Caller:    caller,
					Func:      "Unmarshal",
					GoType:    reflect.TypeOf(v),
					JSONValue: b,
					GoValueV1: val1,
					GoValueV2: val2,
					ErrorV1:   err1,
					ErrorV2:   err2,
					Options:   options,
				})
			}
		}

		// Select the appropriate return value.
		switch mode {
		case CallBothButReturnV1, CallV2ButUponErrorReturnV1:
			c.NumUnmarshalReturnV1.Add(1)
			return err1
		case CallBothButReturnV2, CallV1ButUponErrorReturnV2:
			c.NumUnmarshalReturnV2.Add(1)
			return err2
		}
	}
	panic("unknown mode")
}

// SetMarshalCallRatio sets the ratio of [Codec.Marshal] calls
// that will use the marshal functionality of v1, v2, or both.
//
// The ratio must be within 0 and 1, where:
//   - 0.0 means to use mode1 100% of the time and mode2 0% of the time.
//   - 0.1 means to use mode1 90% of the time and mode2 10% of the time.
//   - 0.5 means to use mode1 50% of the time and mode2 50% of the time.
//   - 0.9 means to use mode1 10% of the time and mode2 90% of the time.
//   - 1.0 means to use mode1 0% of the time and mode2 100% of the time.
//
// For example:
//
//	// This configures marshal to call v1 90% of the time,
//	// but call both both v1 and v2 10% of the time
//	// (while still returning the result of v1).
//	codec.SetMarshalCallRatio(OnlyCallV1, CallBothButReturnV1, 0.1)
//
// By default, marshal will use [OnlyCallV1].
// This is safe to call concurrently with [Codec.Marshal].
func (c *Codec) SetMarshalCallRatio(mode1, mode2 CallMode, ratio float64) {
	c.marshalCallRatio.storeModeRatio(mode1, mode2, float32(ratio))
}

// SetMarshalCallMode specifies the [CallMode] for marshaling.
// By default, marshal will use [OnlyCallV1].
// This is safe to call concurrently with [Codec.Marshal].
func (c *Codec) SetMarshalCallMode(mode CallMode) {
	c.marshalCallRatio.storeModeRatio(mode, mode, 1.0)
}

// MarshalCallRatio retrieves the mode and ratio parameters
// previously set by [Codec.SetMarshalCallRatio].
func (c *Codec) MarshalCallRatio() (mode1, mode2 CallMode, ratio float64) {
	mode1, mode2, ratio32 := c.marshalCallRatio.loadModeRatio()
	return mode1, mode2, float64(ratio32)
}

// SetUnmarshalCallRatio sets the ratio of [Codec.Unmarshal] calls
// that will use the unmarshal functionality of v1, v2, or both.
//
// The ratio must be within 0 and 1, where:
//   - 0.0 means to use mode1 100% of the time and mode2 0% of the time.
//   - 0.1 means to use mode1 90% of the time and mode2 10% of the time.
//   - 0.5 means to use mode1 50% of the time and mode2 50% of the time.
//   - 0.9 means to use mode1 10% of the time and mode2 90% of the time.
//   - 1.0 means to use mode1 0% of the time and mode2 100% of the time.
//
// For example:
//
//	// This configures unmarshal to call v1 90% of the time,
//	// but call both both v1 and v2 10% of the time
//	// (while still returning the result of v1).
//	codec.SetUnmarshalCallRatio(OnlyCallV1, CallBothButReturnV1, 0.1)
//
// By default, unmarshal will only use [OnlyCallV1].
// This is safe to call concurrently with [Codec.Unmarshal].
func (c *Codec) SetUnmarshalCallRatio(mode1, mode2 CallMode, ratio float64) {
	c.unmarshalCallRatio.storeModeRatio(mode1, mode2, float32(ratio))
}

// SetUnmarshalCallMode specifies the [CallMode] for unmarshaling.
// By default, unmarshal will only use [OnlyCallV1].
// This is safe to call concurrently with [Codec.Unmarshal].
func (c *Codec) SetUnmarshalCallMode(mode CallMode) {
	c.unmarshalCallRatio.storeModeRatio(mode, mode, 1.0)
}

// UnmarshalCallRatio retrieves the mode and ratio parameters
// previously set by [Codec.SetUnmarshalCallRatio].
func (c *Codec) UnmarshalCallRatio() (mode1, mode2 CallMode, ratio float64) {
	mode1, mode2, ratio32 := c.unmarshalCallRatio.loadModeRatio()
	return mode1, mode2, float64(ratio32)
}

// callModeRatio non-deterministically determines which call mode to use.
type callModeRatio struct {
	atomic.Uint64 // [0:16) is mode1, [16:32) is mode2, and [32:] is the ratio as raw float32
}

// storeModeRatio stores a call mode ratio.
// See [Codec.SetMarshalCallRatio] or [Codec.SetUnmarshalCallRatio].
func (p *callModeRatio) storeModeRatio(mode1, mode2 CallMode, ratio float32) {
	mode1.checkValid()
	mode2.checkValid()
	if ratio != min(max(0, ratio), 1) {
		panic("ratio out of range")
	}
	u := 0 |
		uint64(mode1&0xffff)<<0 |
		uint64(mode2&0xffff)<<16 |
		uint64(math.Float32bits(float32(ratio)))<<32
	p.Store(u)
}

func (p *callModeRatio) loadModeRatio() (mode1, mode2 CallMode, ratio float32) {
	u := p.Load()
	mode1 = CallMode((u >> 0) & 0xffff)
	mode2 = CallMode((u >> 16) & 0xffff)
	ratio = math.Float32frombits(uint32(u >> 32))
	return mode1, mode2, ratio
}

// loadRandomMode loads a random mode according to the ratio.
func (p *callModeRatio) loadRandomMode() CallMode {
	mode1, mode2, ratio := p.loadModeRatio()
	if ratio < 1 && rand.Float32() >= ratio {
		return mode1
	} else {
		return mode2
	}
}

// ExpVar returns an expvar mapping of all metrics.
// It reports variables with the snake case form of each field in [CodecMetrics].
func (c *CodecMetrics) ExpVar() expvar.Var {
	var m expvar.Map
	v := reflect.ValueOf(c).Elem()
	for i := range v.NumField() {
		name := v.Type().Field(i).Name
		value := v.Field(i).Addr().Interface().(expvar.Var)

		// Convert PascalCase to snake_case.
		var rs []rune
		for i, r := range name {
			if unicode.IsUpper(r) {
				if i > 0 {
					rs = append(rs, '_')
				}
				r = unicode.ToLower(r)
			}
			rs = append(rs, r)
		}
		name = string(rs)

		m.Set(name, value)
	}
	return &m
}

func (c *Codec) jsonEqual(v1, v2 jsontext.Value) bool {
	if c.EqualJSONValues != nil {
		return c.EqualJSONValues(v1, v2)
	}
	return bytes.Equal(v1, v2)
}

func (c *Codec) goEqual(v1, v2 any) bool {
	if c.EqualGoValues != nil {
		return c.EqualGoValues(v1, v2)
	}
	return reflect.DeepEqual(v1, v2)
}

func (c *Codec) errorsEqual(err1, err2 error) bool {
	if c.EqualErrors != nil {
		return c.EqualErrors(err1, err2)
	}
	return (err1 != nil) == (err2 != nil)
}

func (c *Codec) cloneGoValue(v any) any {
	// If possible, use the custom clone function,
	// but fallback on trivial cloning if it returns nil.
	if c.CloneGoValue != nil {
		if v := c.CloneGoValue(v); v != nil {
			return v
		}
	}

	// The only value that can trivially be cloned is a pointer to a zero'd value.
	p := reflect.ValueOf(v)
	if !isPointerToZero(p) {
		return nil
	}
	return reflect.New(p.Elem().Type()).Interface()
}

func isPointerToZero(p reflect.Value) bool {
	return p.Kind() == reflect.Pointer && !p.IsNil() && p.Elem().IsZero()
}

// jsonv1Marshal is like [jsonv1.Marshal],
// but allows specifying options to override default v1 behavior.
func jsonv1Marshal(v any, o ...jsonv2.Options) ([]byte, error) {
	if len(o) == 0 || (len(o) == 1 && o[0] == jsonv1.DefaultOptionsV1()) {
		return jsonv1std.Marshal(v)
	}
	var arr [8]jsonv2.Options
	return jsonv2.Marshal(v, append(append(arr[:0], jsonv1.DefaultOptionsV1()), o...)...)
}

// jsonv1Unmarshal is like [jsonv1.Unmarshal],
// but allows specifying options to override default v1 behavior.
func jsonv1Unmarshal(b []byte, v any, o ...jsonv2.Options) error {
	if len(o) == 0 || (len(o) == 1 && o[0] == jsonv1.DefaultOptionsV1()) {
		return jsonv1std.Unmarshal(b, v)
	}
	var arr [8]jsonv2.Options
	return jsonv2.Unmarshal(b, v, append(append(arr[:0], jsonv1.DefaultOptionsV1()), o...)...)
}

// elapsed measures the duration of calling f.
func elapsed(f func()) time.Duration {
	t := time.Now()
	f()
	return time.Since(t)
}

// shallowCopy shallow copies new to dst if both are non-nil pointers
// and returns a pointer the old value of dst.
func shallowCopy(dst, new any) (old any) {
	dv := reflect.ValueOf(dst)
	nv := reflect.ValueOf(new)
	if dv.Kind() == reflect.Pointer && !dv.IsNil() && nv.Kind() == reflect.Pointer && !nv.IsNil() && dv.Type() == nv.Type() {
		ov := reflect.New(dv.Type().Elem()) // allocate for old value
		ov.Elem().Set(dv.Elem())            // preserve old value
		dv.Elem().Set(nv.Elem())            // insert new value
		return ov.Interface()               // return old value
	}
	return dst
}

// SizeHistogram is a log₂ histogram of sizes.
// Each index i maps to a count of sizes seen within [ 2ⁱ⁻¹ : 2ⁱ ).
type SizeHistogram [bits.UintSize + 1]expvar.Int

func (h *SizeHistogram) insertSize(n int) {
	h[bits.Len(uint(max(n, 0)))].Add(1)
}

// MarshalJSON marshals the histogram as a JSON object where
// each name represents a size range in the format "<N{prefix}B", and
// each value is the count of sizes observed in that range.
//
// The name format is as follows:
//   - N is the upper bound of the size range (2ⁱ) where i is modulo 10.
//   - {prefix} is one of "", "Ki", "Mi", "Gi", "Ti", "Pi", or "Ei",
//     representing binary prefixes for sizes scaled by powers of 2¹⁰.
//   - B denotes bytes.
//
// For example, the name "<64KiB" indicates sizes in the range [32KiB, 64KiB).
// Only ranges with non-zero counts are included in the JSON output.
func (h *SizeHistogram) MarshalJSON() ([]byte, error) {
	var b []byte
	b = append(b, '{')
	const prefixes = "  " + "Ki" + "Mi" + "Gi" + "Ti" + "Pi" + "Ei"
	for i := range h {
		if n := h[i].Value(); n > 0 {
			b = append(b, '"', '<')
			b = strconv.AppendInt(b, 1<<(i%10), 10)
			b = append(b, prefixes[2*(i/10):][:2]...)
			b = bytes.TrimRight(b, " ")
			b = append(b, 'B', '"', ':')
			b = strconv.AppendInt(b, n, 10)
			b = append(b, ',')
		}
	}
	b = bytes.TrimRight(b, ",")
	b = append(b, '}')
	return b, nil
}

// String returns the histogram as JSON.
// It implements both [fmt.Stringer] and [expvar.Var].
func (h *SizeHistogram) String() string {
	b, _ := h.MarshalJSON()
	return string(b)
}

// autoDetectOptions automatically detects which options
// need to be specified to [jsonv2.Marshal] or [jsonv2.Unmarshal]
// in order for it to preserve the same behavior as v1.
//
// The arshalEqual function runs [jsonv2.Marshal] or [jsonv2.Unmarshal]
// function with the provided options and reports whether
// the output is identical to the results from v1.
func autoDetectOptions(arshalEqual func(...jsonv2.Options) bool, o ...jsonv2.Options) jsonv2.Options {
	optsCall := jsonv2.JoinOptions(o...)                              // explicit options by caller
	optsV1 := jsonv2.JoinOptions(jsonv1.DefaultOptionsV1(), optsCall) // caller options using v1 defaults

	// As a sanity check, make sure using v1 options by default is equal to v1.
	// If not, this suggestions that the v1 implementation in terms of v2
	// somehow has a regression bug and the detection logic below will fail.
	if !arshalEqual(optsV1) {
		return nil
	}

	// TODO: The following algorithm runs in O(len(defaultOptionsV1)).
	// This could be O(log₂(len(defaultOptionsV1))) with a binary search.

	// TODO: The [jsonv2.Deterministic] option cannot be reliably detected
	// without multiple runs due to it's non-deterministic nature.

	// TODO: Some options are sub-options of others. A linear search may not
	// properly detected them. For example, [jsonv1.MatchCaseSensitiveDelimiter]
	// is only significant with [jsonv2.MatchCaseInsensitiveNames].

	// Iterate through all the default options for v1 and
	// set just a single v1 option to false and see if it affects equality.
	// If not equal, then it means that this option is significant.
	var opts []jsonv2.Options
	for _, option := range defaultOptionsV1 {
		if _, ok := jsonv2.GetOption(optsCall, option); ok {
			continue // explicitly overwritten by caller, so ignore
		}
		if !arshalEqual(optsV1, option(false)) {
			opts = append(opts, option(true)) // need this option enabled to maintain equality
		}
	}

	return jsonv2.JoinOptions(opts...)
}

// defaultOptionsV1 is the set of all options in [jsonv1.DefaultOptionsV1].
// TODO: We should support a way to iterate through all singular options.
var defaultOptionsV1 = map[string]func(bool) jsonv2.Options{
	"jsontext.AllowDuplicateNames":           jsontext.AllowDuplicateNames,
	"jsontext.AllowInvalidUTF8":              jsontext.AllowInvalidUTF8,
	"jsontext.EscapeForHTML":                 jsontext.EscapeForHTML,
	"jsontext.EscapeForJS":                   jsontext.EscapeForJS,
	"jsontext.PreserveRawStrings":            jsontext.PreserveRawStrings,
	"jsonv1.CallMethodsWithLegacySemantics":  jsonv1.CallMethodsWithLegacySemantics,
	"jsonv1.FormatByteArrayAsArray":          jsonv1.FormatByteArrayAsArray,
	"jsonv1.FormatBytesWithLegacySemantics":  jsonv1.FormatBytesWithLegacySemantics,
	"jsonv1.FormatDurationAsNano":            jsonv1.FormatDurationAsNano,
	"jsonv1.MatchCaseSensitiveDelimiter":     jsonv1.MatchCaseSensitiveDelimiter,
	"jsonv1.MergeWithLegacySemantics":        jsonv1.MergeWithLegacySemantics,
	"jsonv1.OmitEmptyWithLegacySemantics":    jsonv1.OmitEmptyWithLegacySemantics,
	"jsonv1.ParseBytesWithLooseRFC4648":      jsonv1.ParseBytesWithLooseRFC4648,
	"jsonv1.ParseTimeWithLooseRFC3339":       jsonv1.ParseTimeWithLooseRFC3339,
	"jsonv1.ReportErrorsWithLegacySemantics": jsonv1.ReportErrorsWithLegacySemantics,
	"jsonv1.StringifyWithLegacySemantics":    jsonv1.StringifyWithLegacySemantics,
	"jsonv1.UnmarshalArrayFromAnyLength":     jsonv1.UnmarshalArrayFromAnyLength,
	"jsonv2.Deterministic":                   jsonv2.Deterministic,
	"jsonv2.FormatNilMapAsNull":              jsonv2.FormatNilMapAsNull,
	"jsonv2.FormatNilSliceAsNull":            jsonv2.FormatNilSliceAsNull,
	"jsonv2.MatchCaseInsensitiveNames":       jsonv2.MatchCaseInsensitiveNames,
}
