# OrchidDB
Orchid is a simple Key Value database written in Go.

```
      .oooooo.                      oooo         o8o        .o8     oooooooooo.   oooooooooo.
     d8P'  `Y8b                     `888         `''       '888     `888'   `Y8b  `888'   `Y8b    _ (`-`) _
    888      888 oooo d8b  .ooooo.   888 .oo.   oooo   .oooo888      888      888  888     888  /` '.\\ /.' `\\
    888      888 `888''8P d88' `'Y8  888P'Y88b  `888  d88' `888      888      888  888oooo888'  ``'-.,=,.-'``
    888      888  888     888        888   888   888  888   888      888      888  888    `88b    .'//v\\\\'.
    `88b    d88'  888     888   .o8  888   888   888  888   888      888     d88'  888    .88P   (_/\\ \" /\\_)
     `Y8bood8P'  d888b    `Y8bod8P' o888o o888o o888o `Y8bod88P'    o888bood8P'   o888bood8P'        '-'
```

Orchid is a work in progress KV database.

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
