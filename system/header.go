package system

import (
	"fmt"
	"strings"
	"unicode/utf8"

	"orchiddb/globals"
)

var line1 string = "  .oooooo.                      oooo         o8o        .o8       oooooooooo.   oooooooooo.  "
var line2 string = " d8P'  `Y8b                     `888         `''       '888       `888'   `Y8b  `888'   `Y8b "
var line3 string = "888      888 oooo d8b  .ooooo.   888 .oo.   oooo   .oooo888        888      888  888     888 "
var line4 string = "888      888 `888''8P d88' `'Y8  888P'Y88b  `888  d88' `888        888      888  888oooo888' "
var line5 string = "888      888  888     888        888   888   888  888   888        888      888  888    `88b "
var line6 string = "`88b    d88'  888     888   .o8  888   888   888  888   888        888     d88'  888    .88P "
var line7 string = " `Y8bood8P'  d888b    `Y8bod8P' o888o o888o o888o `Y8bod88P'      o888bood8P'   o888bood8P'  "

var lines []string = []string{line1, line2, line3, line4, line5, line6, line7}

var producedBy string = fmt.Sprintf("A %s product.", globals.Developer)
var disclaimer string = "Orchid is a work in progress in-memory KV database."

func printHeader() {
	width := getOutputWidth()
	vis := utf8.RuneCountInString(line1)
	spacer := (width - vis) / 2
	prefix := strings.Repeat(" ", spacer)
	for _, v := range lines {
		fmt.Println(prefix, v)
	}
}

func printVersion(major, minor, patch int) {
	version := fmt.Sprintf("%d.%d.%d", major, minor, patch)
	verString := fmt.Sprintf("Running verison: %s", version)
	fmt.Println(verString)
}

func PrintStartupText(maj, min, patch int) {
	PrintAsciiLine()
	printHeader()
	PrintAsciiLine()
	fmt.Println(producedBy)
	printVersion(maj, min, patch)
	PrintAsciiLine()
	fmt.Println(disclaimer)
	PrintAsciiLine()
	fmt.Println()
}
