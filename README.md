# OrchidDB
Orchid is a simple Key Value database written in Go.

## Query Language

* `MAKE(table)`
* `DROP(table)`
* `GET(table, key)`
* `PUT(table, key, value)`
* `DEL(table, key)`

## Runtime Options (CLI)
	
* `-path`      `string`   Path to place database files. Ideally is empty directory.
* `-addr`      `string`   Which address the server uses for listening. Defaults to 127.0.0.1.
* `-port`      `int`      Which port the server usese for listening. Defaults to 6000.
* `-page-size` `int`      Size in bytes for a single database page. Defaults to OS page size.
* `-node-min`  `float32`  Minimum percentage a node must be filled to before consolidation.
* `-node-max`  `float32`  Maximum percentage a node must be to before splitting.
