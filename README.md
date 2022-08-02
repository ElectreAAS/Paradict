# ParaDict, an OCaml implementation of CTries for parallel dictionnaries

# THIS IS A WORK IN PROGRESS

## Differences between this repo and the original
By original I mean the code described on wikipedia [here](https://en.wikipedia.org/wiki/Ctrie), and in the papers in the notes 1 and 2 on that page.

- On wiki there is this line `bit = bmp & (1 << ((hashcode >> level) & 0x1F))` which is imho incorrect, as the `and bmp` is an error here and redundant with the `and` in the `iinsert` function.
- In the paper they talk about a "32-bit hashcode space". I use [Digestif](https://github.com/mirage/digestif)'s SHA256 hash algorithm, and the hashcode space is 256-bit. This only expands the maximum depth from 6 to 51, rendering the possibility of a full hash collision insignificant.
