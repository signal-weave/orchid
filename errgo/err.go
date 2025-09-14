package errgo

import "fmt"

// The error must not equal nil.
// For use with functions that only return an error or nil.
// Will panic if err != nil.
func PanicIfError(err error) {
	if err != nil {
		fmt.Println(err)
		panic(err)
	}
}
