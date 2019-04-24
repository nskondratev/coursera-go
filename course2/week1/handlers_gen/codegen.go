package main

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"log"
	"os"
	"strings"
	"text/template"
)

// код писать тут

type codegenParams struct {
	Url      string `json:"url"`
	Auth     bool   `json:"auth"`
	Method   string `json:"method"`
	FuncName string `json:"-"`
}

func newCodegenParamsFromJSON(b []byte) (*codegenParams, error) {
	c := &codegenParams{}
	if err := json.Unmarshal(b, &c); err != nil {
		return c, err
	}
	if len(c.Method) < 1 {
		c.Method = "GET"
	}
	return c, nil
}

type serveHTTPMethodsHub map[string][]*codegenParams

func (h serveHTTPMethodsHub) AddHandlerForStruct(sn string, cp *codegenParams) {
	if _, ok := h[sn]; !ok {
		h[sn] = make([]*codegenParams, 0, 1)
	}
	h[sn] = append(h[sn], cp)
}

func (h serveHTTPMethodsHub) String() string {
	sb := &strings.Builder{}
	sb.WriteString("{ ")
	for sn, cps := range h {
		sb.WriteString(sn + " : [")
		for _, cp := range cps {
			sb.WriteString(fmt.Sprintf("%+v, ", cp))
		}
		sb.WriteString("] ")
	}
	sb.WriteString("}")
	return sb.String()
}

const apiGenPrefix = "// apigen:api "

type handlerTplParams struct {
	StructName string
	MethodName string
	Auth       bool
	HttpMethod string
}

type httpTplParams struct {
	StructName    string
	CodegenParams []*codegenParams
}

var (
	handlerTpl = template.Must(template.New("handlerTpl").Parse(`
func (h *{{.StructName}}) handler{{.MethodName}}(w http.ResponseWriter, r *http.Request) {
	{{if .Auth}}
	if strings.Compare(r.Header.Get("X-Auth"), "100500") != 0 {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte("{\"error\":\"unauthorized\"}"))
		return
	}
	{{- end}}
	if r.Method != "{{.HttpMethod}}" {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte("{\"error\":\"bad method\"}"))
		return
	}
	// Fill params
	// Validate params
	ctx := context.Background()
	res, err := h.{{.MethodName}}(ctx, params)
	if err != nil {
		c := http.StatusInternalServerError
		e := err.Error()
		if err, ok := err.(ApiError); ok {
			c = err.HTTPStatus
		}
		w.WriteHeader(c)
		rb, _ := json.Marshal(map[string]string{"error": e})
		_, _ = w.Write(rb)
	}
	rb, _ := json.Marshal(res)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(rb)
}
`))

	httpTpl = template.Must(template.New("httpTpl").Parse(`
func (h *{{.StructName}}) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	{{range $cp := .CodegenParams}}case "{{$cp.Url}}":
		h.{{$cp.FuncName}}(w, r)
	{{end}}default:
		w.WriteHeader(http.StatusNotFound)
		rb, _ := json.Marshal(map[string]string{"error": "unknown method"})
		_, _ = w.Write(rb)
	}
}
`))
)

func main() {
	handlersHub := make(serveHTTPMethodsHub)
	_ = handlersHub // TODO: Delete

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

	if _, err := fmt.Fprintln(out, `import "context"`); err != nil {
		log.Fatal(err)
	}

	if _, err := fmt.Fprintln(out, `import "encoding/json"`); err != nil {
		log.Fatal(err)
	}

	if _, err := fmt.Fprintln(out, `import "net/http"`); err != nil {
		log.Fatal(err)
	}

	if _, err := fmt.Fprintln(out, `import "strings"`); err != nil {
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
		var cp *codegenParams
		for _, comment := range fn.Doc.List {
			if strings.HasPrefix(comment.Text, apiGenPrefix) {
				needCodegen = true
				if cp, err = newCodegenParamsFromJSON([]byte(strings.TrimPrefix(comment.Text, apiGenPrefix))); err != nil {
					log.Fatalf("FATAL incorrect apigen params for func %s, params: %s", fn.Name.Name, comment.Text)
				}
			}
		}
		if !needCodegen {
			fmt.Printf("SKIP func %s does not have // apigen:api comment\n", fn.Name.Name)
		}
		fmt.Printf("GENERATE api wrapper for func %s, codegen params: %+v\n", fn.Name.Name, cp)
		cp.FuncName = "handler" + fn.Name.Name
		structName := fn.Recv.List[0].Type.(*ast.StarExpr).X.(*ast.Ident).Name
		handlersHub.AddHandlerForStruct(structName, cp)
		if err := handlerTpl.Execute(out, handlerTplParams{structName, fn.Name.Name, cp.Auth, cp.Method}); err != nil {
			log.Fatal(err)
		}
	}
	fmt.Printf("Methods hub: %s\n", handlersHub)

	// Generate ServeHTTP method for structs
	for sn, cp := range handlersHub {
		if err := httpTpl.Execute(out, httpTplParams{sn, cp}); err != nil {
			log.Fatal(err)
		}
	}

}
