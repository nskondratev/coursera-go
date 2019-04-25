package main

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"log"
	"os"
	"reflect"
	"strconv"
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

type validateParams struct {
	FieldName string
	FieldType string
	Required  bool
	ParamName string
	Enum      []string
	Default   string
	Min       *int64
	Max       int64
}

func (vp *validateParams) GetValueFromRequest(httpMethod string) string {
	res := "r.FormValue(\"" + vp.ParamName + "\")"

	rawVarName := "raw" + vp.FieldName

	switch vp.FieldType {
	case "int":
		res = rawVarName + ", err := strconv.Atoi(" + res + ")"
		res = res + `
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		rb, _ := json.Marshal(&ResponseEnvelope{"` + vp.ParamName + ` must be int",nil})
		_, _ = w.Write(rb)
		return
	}
`
	case "string":
		res = rawVarName + " := " + res
	}
	return res
}

func (vp *validateParams) GetValidation() string {
	res := ""
	rawVarName := "raw" + vp.FieldName
	if vp.Required {
		switch vp.FieldType {
		case "int":
			res = `if ` + rawVarName + ` == 0 {
		w.WriteHeader(http.StatusBadRequest)
		rb, _ := json.Marshal(&ResponseEnvelope{"` + vp.ParamName + ` must me not empty",nil})
		_, _ = w.Write(rb)
		return
	}
`
		case "string":
			res = `if len(` + rawVarName + `) < 1 {
		w.WriteHeader(http.StatusBadRequest)
		rb, _ := json.Marshal(&ResponseEnvelope{"` + vp.ParamName + ` must me not empty",nil})
		_, _ = w.Write(rb)
		return
	}
`
		}
	}

	if len(vp.Default) > 0 {
		switch vp.FieldType {
		case "int":
			dn, err := strconv.Atoi(vp.Default)
			if err == nil && dn > 0 {
				res = res + `
	if ` + rawVarName + ` == 0 {
		` + rawVarName + ` = ` + vp.Default + `
	}
`
			}
		case "string":
			res = res + `
	if len(` + rawVarName + `) < 1 {
		` + rawVarName + ` = "` + vp.Default + `"
	}
`
		}
	}

	if len(vp.Enum) > 0 {
		sb := strings.Builder{}
		for i, v := range vp.Enum {
			if i > 0 {
				sb.WriteString(" && ")
			}
			sb.WriteString(rawVarName)
			sb.WriteString(" != \"")
			sb.WriteString(v)
			sb.WriteString("\"")
		}
		res = res + `
	if ` + sb.String() + ` {
		w.WriteHeader(http.StatusBadRequest)
		rb, _ := json.Marshal(&ResponseEnvelope{"` + vp.ParamName + ` must be one of [` + strings.Join(vp.Enum, ", ") + `]",nil})
		_, _ = w.Write(rb)
		return
	}
`
	}

	if vp.Min != nil {
		switch vp.FieldType {
		case "int":
			res = res + `
	if ` + rawVarName + ` < ` + strconv.FormatInt(*vp.Min, 10) + ` {
		w.WriteHeader(http.StatusBadRequest)
		rb, _ := json.Marshal(&ResponseEnvelope{"` + vp.ParamName + ` must be >= ` + strconv.FormatInt(*vp.Min, 10) + `",nil})
		_, _ = w.Write(rb)
		return
	}
`
		case "string":
			res = res + `
	if len(` + rawVarName + `) < ` + strconv.FormatInt(*vp.Min, 10) + ` {
		w.WriteHeader(http.StatusBadRequest)
		rb, _ := json.Marshal(&ResponseEnvelope{"` + vp.ParamName + ` len must be >= ` + strconv.FormatInt(*vp.Min, 10) + `",nil})
		_, _ = w.Write(rb)
		return
	}
`
		}
	}

	if vp.Max > 0 {
		switch vp.FieldType {
		case "int":
			res = res + `
	if ` + rawVarName + ` > ` + strconv.FormatInt(vp.Max, 10) + ` {
		w.WriteHeader(http.StatusBadRequest)
		rb, _ := json.Marshal(&ResponseEnvelope{"` + vp.ParamName + ` must be <= ` + strconv.FormatInt(vp.Max, 10) + `",nil})
		_, _ = w.Write(rb)
		return
	}
`
		}
	}

	return res
}

type handlerTplParams struct {
	StructName     string
	MethodName     string
	Auth           bool
	HttpMethod     string
	ParamTypeName  string
	ValidateParams []*validateParams
}

type httpTplParams struct {
	StructName    string
	CodegenParams []*codegenParams
}

var (
	handlerTpl = template.Must(template.New("handlerTpl").Parse(`
func (h *{{.StructName}}) handler{{.MethodName}}(w http.ResponseWriter, r *http.Request) {
	{{if ne .HttpMethod ""}}if r.Method != "{{.HttpMethod}}" {
		w.WriteHeader(http.StatusNotAcceptable)
		_, _ = w.Write([]byte("{\"error\":\"bad method\"}"))
		return
	}{{end}}
	{{if .Auth}}
	if strings.Compare(r.Header.Get("X-Auth"), "100500") != 0 {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte("{\"error\":\"unauthorized\"}"))
		return
	}
	{{end}}
	// Fill params
	params := {{.ParamTypeName}}{}
	{{range $f := .ValidateParams}}
	{{$f.GetValueFromRequest $.HttpMethod}}
	{{$f.GetValidation}}
	params.{{$f.FieldName}} = raw{{$f.FieldName}}
	{{end}}
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
		return
	}
	rb, _ := json.Marshal(&ResponseEnvelope{"", res})
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

	resEnvelope = `
type ResponseEnvelope struct {
	Error string ` + "`json:\"error\"`" + `
	Response interface{} ` + "`json:\"response,omitempty\"`" + `
}
`
)

func main() {
	handlersHub := make(serveHTTPMethodsHub)

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

	if _, err := fmt.Fprintln(out, `import "strconv"`); err != nil {
		log.Fatal(err)
	}

	if _, err := fmt.Fprintln(out, `import "strings"`); err != nil {
		log.Fatal(err)
	}

	if _, err := fmt.Fprintln(out); err != nil { // empty line
		log.Fatal(err)
	}

	if _, err := fmt.Fprint(out, resEnvelope); err != nil {
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

		// Parse second argument
		at := fn.Type.Params.List[1].Type.(*ast.Ident).Obj.Decl.(*ast.TypeSpec)
		argStructName := at.Name.Name
		argStructFields := at.Type.(*ast.StructType).Fields.List

		log.Printf("METHOD ARGUMENT: %s, %s, %#v", fn.Name.Name, argStructName, argStructFields)

		vp := make([]*validateParams, len(argStructFields), len(argStructFields))

		for i, field := range argStructFields {
			fieldName := field.Names[0].Name
			log.Printf("Process %s.%s field", argStructName, fieldName)

			if field.Tag == nil {
				log.Printf("Skip %s.%s field as there is no any tags", argStructName, fieldName)
				continue
			}

			tag := reflect.StructTag(field.Tag.Value[1 : len(field.Tag.Value)-1])
			if len(tag.Get("apivalidator")) < 1 {
				log.Printf("Skip %s.%s field as there is no tag apivalidator", argStructName, fieldName)
				continue
			}

			fieldType := field.Type.(*ast.Ident).Name

			if fieldType != "int" && fieldType != "string" {
				log.Printf("Skip %s.%s field as it is not int or string. Type: %s", argStructName, fieldName, fieldType)
			}

			v := &validateParams{
				FieldName: fieldName,
				FieldType: fieldType,
				ParamName: strings.ToLower(fieldName),
			}

			tagArgs := strings.Split(tag.Get("apivalidator"), ",")

			for _, tagArg := range tagArgs {
				log.Printf("Process tag arg: %s", tagArg)
				tagTokens := strings.Split(tagArg, "=")
				switch tagTokens[0] {
				case "required":
					v.Required = true
				case "paramname":
					v.ParamName = tagTokens[1]
				case "enum":
					v.Enum = strings.Split(tagTokens[1], "|")
					//v.Required = true
				case "default":
					v.Default = tagTokens[1]
				case "min":
					num, _ := strconv.ParseInt(tagTokens[1], 10, 64)
					log.Printf("Parsed min tag value: %d", num)
					v.Min = &num
				case "max":
					num, _ := strconv.ParseInt(tagTokens[1], 10, 64)
					v.Max = num
				}
			}

			log.Printf("Constructed validateParams for field %s.%s: %#v", argStructName, fieldName, v)

			vp[i] = v
		}

		if err := handlerTpl.Execute(out, handlerTplParams{structName, fn.Name.Name, cp.Auth, cp.Method, argStructName, vp}); err != nil {
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
