// Copyright 2025 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package jsonsplit_test

import (
	"fmt"
	"log"
	"slices"

	"github.com/go-json-experiment/jsonsplit"
)

// Example of calling marshal and unmarshal with v1 semantics,
// but being able to detect differences between v1 and v2.
func Example() {
	c := jsonsplit.Codec{
		// When differences between v1 and v2 are detected, try to also
		// detect which specific options are causing the difference.
		AutoDetectOptions: true,

		// Print out the detected differences between v1 and v2.
		ReportDifference: func(d jsonsplit.Difference) {
			switch d.Func {
			case "Marshal":
				fmt.Printf("Marshal difference detected:\n"+
					"\tGoValue:     %+v\n"+
					"\tJSONValueV1: %s\n"+
					"\tJSONValueV2: %s\n"+
					"\tOptions:     %v\n",
					d.GoValue, d.JSONValueV1, d.JSONValueV2, slices.Collect(d.OptionNames()))
			case "Unmarshal":
				fmt.Printf("Unmarshal difference detected:\n"+
					"\tJSONValue:   %s\n"+
					"\tGoValueV1:   %+v\n"+
					"\tGoValueV2:   %+v\n"+
					"\tOptions:     %v\n",
					d.JSONValue, d.GoValueV1, d.GoValueV2, slices.Collect(d.OptionNames()))
			}
		},
	}

	// Specify that marshal/unmarshal should call both v1 and v2,
	// but continue to return the results of v1.
	c.SetMarshalCallMode(jsonsplit.CallBothButReturnV1)
	c.SetUnmarshalCallMode(jsonsplit.CallBothButReturnV1)

	const in = `{"FIRSTNAME":"John","LASTNAME":"Doe","lastName":"Dupe"}`
	type User struct {
		FirstName string   `json:"firstName"`
		LastName  string   `json:"lastName"`
		Age       int      `json:"age,omitempty"`
		Aliases   []string `json:"tags"`
	}
	var u User

	// Unmarshal according to v1 semantics, which will:
	//   - match JSON object names case-insensitively
	//   - allow duplicate JSON object names
	if err := c.Unmarshal([]byte(in), &u); err != nil {
		log.Fatal(err)
	}

	// Marshal according to v1 semantics, which will:
	//   - emit Age since omitempty works with integers in v1
	//   - emit Aliases as a JSON null instead of a []
	if _, err := c.Marshal(u); err != nil {
		log.Fatal(err)
	}

	// Output:
	// Unmarshal difference detected:
	// 	JSONValue:   {"FIRSTNAME":"John","LASTNAME":"Doe","lastName":"Dupe"}
	// 	GoValueV1:   &{FirstName:John LastName:Dupe Age:0 Aliases:[]}
	// 	GoValueV2:   &{FirstName: LastName:Dupe Age:0 Aliases:[]}
	// 	Options:     [jsontext.AllowDuplicateNames jsonv2.MatchCaseInsensitiveNames]
	// Marshal difference detected:
	// 	GoValue:     {FirstName:John LastName:Dupe Age:0 Aliases:[]}
	// 	JSONValueV1: {"firstName":"John","lastName":"Dupe","tags":null}
	// 	JSONValueV2: {"firstName":"John","lastName":"Dupe","age":0,"tags":[]}
	// 	Options:     [jsonv1.OmitEmptyWithLegacySemantics jsonv2.FormatNilSliceAsNull]
}
