package execution

import (
	"fmt"

	"orchiddb/parser"
	"orchiddb/storage"
)

// A "TableWorker" is a struct that runs a loop over a channel on a goroutine.
// The worker is the primary interface to the underlying table allowing each
// table the io capabilities of a separate OS thread.
// This means Orchid will have n threads where n is the number of active
// tables.
type TableWorker struct {
	// Replace with type to receive. Must be pointer, but to any type.
	in chan *parser.Command

	tbl *storage.Table

	IsIdle bool // Is the loop paused?
}

func NewWorker(tbl *storage.Table) *TableWorker {
	worker := &TableWorker{
		tbl:    tbl,
		IsIdle: true,
	}
	LoadedWorkers[tbl.Name] = worker

	return worker
}

// Start starts the worker loop on a go routine.
func (tw *TableWorker) Start() { tw.IsIdle = false; go tw.loop() }

// Stop closes the in channel and then the worker is idled.
func (tw *TableWorker) Stop() { close(tw.in); tw.IsIdle = true }

// The primary logic for handling parsed commands and turning them into queried
// database data.
func (tw *TableWorker) loop() {
	for cmd := range tw.in {
		if cmd == nil {
			continue
		}

		err := tw.executeCommand(cmd)
		if err != nil {
			fmt.Println(err.Error())
		}
	}
}

func (tw *TableWorker) executeCommand(cmd *parser.Command) error {
	switch t := cmd.Command.(type) {
	case *parser.GetCommand:
		return tw.get(t)
	case *parser.PutCommand:
		return tw.put(t)
	case *parser.DelCommand:
		return tw.del(t)
	default:
		return fmt.Errorf("unknown command: %s", cmd.Command.String())
	}
}

func (tw *TableWorker) get(cmd *parser.GetCommand) error {
	item, err := tw.tbl.Get([]byte(cmd.Key))
	if err != nil {
		return err
	}

	_, err = cmd.Conn.Write(item.Value)
	return err
}

func (tw *TableWorker) put(cmd *parser.PutCommand) error {
	return tw.tbl.Put([]byte(cmd.Key), []byte(cmd.Value))
}

func (tw *TableWorker) del(cmd *parser.DelCommand) error {
	return tw.tbl.Del([]byte(cmd.Key))
}
