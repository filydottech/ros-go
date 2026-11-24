package routeros

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"testing"
)

var readmeCompileExamples = regexp.MustCompile("(?s)<!-- docs:compile -->\\s*```go\\n(.*?)\\n```\\s*<!-- /docs:compile -->")

func TestREADMEExamplesCompile(t *testing.T) {
	readme, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatalf("ReadFile(README.md) error = %v", err)
	}
	examples := readmeCompileExamples.FindAllSubmatch(readme, -1)
	if len(examples) == 0 {
		t.Fatal("README.md has no marked Go examples to compile")
	}

	moduleRoot, err := filepath.Abs(".")
	if err != nil {
		t.Fatalf("Abs(.) error = %v", err)
	}
	for index, example := range examples {
		t.Run(fmt.Sprintf("example_%d", index+1), func(t *testing.T) {
			dir := t.TempDir()
			goMod := fmt.Sprintf("module readmeexample\n\ngo 1.24\n\nrequire github.com/filydottech/ros-go v0.0.0\n\nreplace github.com/filydottech/ros-go => %s\n", moduleRoot)
			if err := os.WriteFile(filepath.Join(dir, "go.mod"), []byte(goMod), 0o600); err != nil {
				t.Fatalf("WriteFile(go.mod) error = %v", err)
			}
			if err := os.WriteFile(filepath.Join(dir, "main.go"), example[1], 0o600); err != nil {
				t.Fatalf("WriteFile(main.go) error = %v", err)
			}

			command := exec.Command("go", "build", "-mod=mod", ".")
			command.Dir = dir
			output, err := command.CombinedOutput()
			if err != nil {
				t.Fatalf("README example does not compile: %v\n%s", err, output)
			}
		})
	}
}
