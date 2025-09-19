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

	tbl    *storage.Table
	IsIdle bool // Is the loop paused?
}

func NewWorker(tbl *storage.Table) *TableWorker {
	worker := &TableWorker{
		tbl:    tbl,
		in:     make(chan *parser.Command, 128),
		IsIdle: true,
	}

	LoadedWorkers[tbl.Name] = worker
	PrintWorkers()

	return worker
}

// Start starts the worker loop on a go routine.
func (tw *TableWorker) Start() {
	if !tw.IsIdle {
		return // already started, prevent double goroutines.
	}

	tw.IsIdle = false
	go tw.loop()
}

// Stop closes the in channel and then the worker is idled.
func (tw *TableWorker) Stop() { close(tw.in); tw.IsIdle = true }

// Close closes the worker's table, returns any error.
func (tw *TableWorker) Close() error { return tw.tbl.Close() }

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
	var item *storage.Item
	var err error
	var resp []byte

	item, err = tw.tbl.Get([]byte(cmd.Key))
	if err != nil {
		return err
	}
	if item == nil {
		resp = []byte("nil")
	} else {
		resp = item.Value
	}

	_, err = cmd.Conn.Write(resp)
	return err
}

func (tw *TableWorker) put(cmd *parser.PutCommand) error {
	err := tw.tbl.Put([]byte(cmd.Key), []byte(cmd.Value))
	if err != nil {
		return err
	}
	return tw.tbl.Txn.Commit()
}

func (tw *TableWorker) del(cmd *parser.DelCommand) error {
	err := tw.tbl.Del([]byte(cmd.Key))
	if err != nil {
		return err
	}
	return tw.tbl.Txn.Commit()
}
