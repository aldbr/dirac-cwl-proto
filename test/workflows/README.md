# Supported workflows

- **helloworld**: 1 `CommandLineTools`

```
o
```

- **crypto**: 1 `Workflow` with 4 independent `CommandLineTools`

```
o  o  o  o
```

- **pi**: 1 linear `Workflow` with 2 dependent `CommandLineTools`

```
o
|
o
```

- **lhcb**: 1 linear `Workflow` with 1 `CommandLineTools` followed by 1 `Workflows` of many dependent `CommandLineTools`

```
o
|
(o->o->o->o->o)
```

- **mandelbrot**: 1 `Workflow` with 2 dependent `Workflows` of 2 `CommandLineTools`

```
(o->o)
  |
(o->o)
```

- **gaussian-fit**: 1 `Workflow` with 2 dependent `Workflow`, where each of them is composed of 2 independent `CommandLineTools`

```
(o  o)
  |
(o  o)
```

# Not tested yet

- 1 `Workflow` with 3 indepent `CommandLineTools` and 1 subsequent `CommandLineTools` that merges the result (tested in PR #10 even if in sligthly smaller shape (2 independent CLT and 1 subsequent CTL)

```
o  o  o
 \ | /
   o
```

- 1 `Workflow` with 1 `CommandLineTools` followed by 2 independent `CommandLineTools` ending in 1 `CommandLineTools`

```
  o
 / \
o   o
 \ /
  o
```
