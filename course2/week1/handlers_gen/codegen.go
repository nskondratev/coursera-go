package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"log"
	"os"
	"strings"
)

// код писать тут

func main() {
	fset := token.NewFileSet()
	node, err := parser.ParseFile(fset, os.Args[1], nil, parser.ParseComments)
	if err != nil {
		log.Fatal(err)
	}

	out, _ := os.Create(os.Args[2])

	if _, err := fmt.Fprintln(out, `package `+node.Name.Name); err != nil {
		log.Fatal(err)
	}

	if _, err := fmt.Fprintln(out); err != nil { // empty line
		log.Fatal(err)
	}

	if _, err := fmt.Fprintln(out, `import "net/http"`); err != nil {
		log.Fatal(err)
	}

	if _, err := fmt.Fprintln(out); err != nil { // empty line
		log.Fatal(err)
	}

	// Parse
	for _, f := range node.Decls {
		fn, ok := f.(*ast.FuncDecl)
		if !ok {
			fmt.Printf("SKIP %T is not ast.FuncDecl\n", f)
			continue
		}

		if fn.Doc == nil {
			fmt.Printf("SKIP func %s does not have comments\n", fn.Name.Name)
			continue
		}
		needCodegen := false
		for _, comment := range fn.Doc.List {
			needCodegen = needCodegen || strings.HasPrefix(comment.Text, "// apigen:api")
		}
		if !needCodegen {
			fmt.Printf("SKIP func %s does not have // apigen:api comment\n", fn.Name.Name)
		}

		fmt.Printf("GENERATE api wrapper for func %s\n", fn.Name.Name)
	}

}
