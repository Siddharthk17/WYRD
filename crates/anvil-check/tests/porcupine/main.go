package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/anishathalye/porcupine"
)

type regInput struct {
	Kind     string
	Key      string
	Value    string
	Expected uint64
}

type regOutput struct {
	Value    *string
	Version  uint64
	Conflict bool
	Empty    bool
}

type regState struct {
	Value   *string
	Version uint64
	Known   bool
}

func strPtr(s string) *string {
	v := s
	return &v
}

func strEq(a, b *string) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

func registerModel() porcupine.Model {
	return porcupine.Model{
		Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
			groups := map[string][]porcupine.Operation{}
			var order []string
			for _, op := range history {
				in := op.Input.(regInput)
				if _, ok := groups[in.Key]; !ok {
					order = append(order, in.Key)
				}
				groups[in.Key] = append(groups[in.Key], op)
			}
			out := make([][]porcupine.Operation, 0, len(order))
			for _, key := range order {
				out = append(out, groups[key])
			}
			return out
		},
		Init: func() interface{} {
			return regState{Value: nil, Version: 0, Known: false}
		},
		Step: func(state interface{}, input interface{}, output interface{}) (bool, interface{}) {
			st := state.(regState)
			in := input.(regInput)
			out := output.(regOutput)
			if out.Empty {
				return true, st
			}
			switch in.Kind {
			case "put":
				if out.Conflict || !strEq(out.Value, strPtr(in.Value)) {
					return false, st
				}
				return true, regState{Value: strPtr(in.Value), Version: out.Version, Known: true}
			case "get":
				if !strEq(out.Value, st.Value) {
					return false, st
				}
				if st.Known {
					if out.Conflict || out.Version != st.Version {
						return false, st
					}
					return true, st
				}
				return true, regState{Value: st.Value, Version: out.Version, Known: true}
			case "delete":
				if out.Conflict || out.Value != nil {
					return false, st
				}
				return true, regState{Value: nil, Version: out.Version, Known: true}
			case "cas":
				if out.Conflict {
					if st.Known {
						if in.Expected == st.Version || out.Version != st.Version {
							return false, st
						}
						return true, st
					}
					if in.Expected == out.Version {
						return false, st
					}
					return true, regState{Value: st.Value, Version: out.Version, Known: true}
				}
				if !strEq(out.Value, strPtr(in.Value)) {
					return false, st
				}
				if st.Known {
					if in.Expected != st.Version {
						return false, st
					}
				} else {
					st.Version = in.Expected
					st.Known = true
				}
				return true, regState{Value: strPtr(in.Value), Version: out.Version, Known: true}
			}
			return false, st
		},
	}
}

type resultValue struct {
	Value    *string `json:"value"`
	Version  uint64  `json:"version"`
	Conflict bool    `json:"conflict"`
}

type rawHistoryOp struct {
	Client   int                        `json:"client"`
	Invoke   map[string]json.RawMessage `json:"invoke"`
	Value    json.RawMessage            `json:"value"`
	Result   *resultValue               `json:"result"`
	InvokeT  int64                      `json:"invoke_time"`
	RetT     int64                      `json:"ret_time"`
}

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: porcupine-check <histories.jsonl>")
		os.Exit(2)
	}
	file, err := os.Open(os.Args[1])
	if err != nil {
		fmt.Fprintf(os.Stderr, "open: %v\n", err)
		os.Exit(1)
	}
	defer file.Close()
	model := registerModel()
	writer := bufio.NewWriter(os.Stdout)
	defer writer.Flush()
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)
	for scanner.Scan() {
		line := scanner.Bytes()
		var rawOps []json.RawMessage
		if err := json.Unmarshal(line, &rawOps); err != nil {
			fmt.Fprintf(os.Stderr, "parse history: %v\n", err)
			os.Exit(1)
		}
		ops := make([]porcupine.Operation, 0, len(rawOps))
		for _, raw := range rawOps {
			var rop rawHistoryOp
			if err := json.Unmarshal(raw, &rop); err != nil {
				fmt.Fprintf(os.Stderr, "parse history op: %v\n", err)
				os.Exit(1)
			}
			var call struct {
				Kind     string `json:"kind"`
				Key      string `json:"key"`
				Value    string `json:"value"`
				Expected uint64 `json:"expected"`
				Limit    int    `json:"limit"`
			}
			invokeOp, ok := rop.Invoke["op"]
			if !ok {
				fmt.Fprintln(os.Stderr, "history op missing invoke.op")
				os.Exit(1)
			}
			if err := json.Unmarshal(invokeOp, &call); err != nil {
				fmt.Fprintf(os.Stderr, "parse call: %v\n", err)
				os.Exit(1)
			}
			// Put, Cas, and Append carry their value in the outer
			// `value` field, not inside `invoke.op`.
			var callValue struct {
				Value string `json:"value"`
			}
			if err := json.Unmarshal(rop.Value, &callValue); err == nil && callValue.Value != "" {
				call.Value = callValue.Value
			}
			in := regInput{Kind: call.Kind, Key: call.Key, Value: call.Value, Expected: call.Expected}
			var out regOutput
			if rop.Result == nil {
				out.Empty = true
			} else if rop.Result.Conflict {
				out.Conflict = true
				out.Version = rop.Result.Version
			} else {
				out.Value = rop.Result.Value
				out.Version = rop.Result.Version
			}
			ops = append(ops, porcupine.Operation{
				ClientId: rop.Client,
				Input:    in,
				Call:     rop.InvokeT,
				Output:   out,
				Return:   rop.RetT,
			})
		}
		res := porcupine.CheckOperationsTimeout(model, ops, 10*time.Second)
		if res == porcupine.Ok {
			fmt.Fprintln(writer, "true")
		} else if res == porcupine.Illegal {
			fmt.Fprintln(writer, "false")
		} else {
			fmt.Fprintf(os.Stderr, "porcupine timeout on history\n")
			os.Exit(1)
		}
	}
	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "scan: %v\n", err)
		os.Exit(1)
	}
}
