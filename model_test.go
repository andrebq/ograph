package ograph

import (
	"fmt"
	"testing"
	"github.com/andrebq/ograph/data"
	"reflect"
	"time"
)

type (
	__fatalF interface {
		Fatalf(fmt string, args ...interface{})
	}
)

func mustOpenGraph(t __fatalF) *G {
	repo := data.Repo{}
	if err := repo.Connect("ograph", "ograph", "ograph", "localhost"); err != nil {
		t.Fatalf("error connecting to repository: %v", err)
	}
	if err := repo.Drop(); err != nil {
		t.Fatalf("error cleaning repo to a empty state: %v", err)
	}
	if err := repo.Create(); err != nil {
		t.Fatalf("error creating the initial repo: %v", err)
	}
	g := &G{}
	g.Use(&repo)
	return g
}

func TestSave(t *testing.T) {
	g := mustOpenGraph(t)
	defer g.Close()
	node := &Node{
		Name: "neo",
	}

	if err := g.SaveAll(node); err != nil {
		t.Fatalf("error saving node: %v", err)
	}

	if out, err := g.Node(node.Gid, "", nil); err != nil {
		t.Fatalf("error searching node by Gid: %v", err)
	} else {
		if !reflect.DeepEqual(out, node) {
			t.Fatalf("invalid response. expecting %v got %v", node, out)
		}
	}
}

func TestSaveRelation(t *testing.T) {
	g := mustOpenGraph(t)
	defer g.Close()
	neo := &Node {
		Name: "neo",
	}
	morpheus := &Node {
		Name: "morpheus",
	}
	rel := neo.Rel("knows", morpheus)
	if err := g.SaveAll(neo, morpheus, rel); err != nil {
		t.Fatalf("error saving all: %v", err)
	}
}

func TestWalk(t *testing.T) {
	g := mustOpenGraph(t)
	defer g.Close()

	neo := &Node{
		Name: "neo",
	}
	morpheus := &Node{
		Name: "morpheus",
	}
	rel := neo.Rel("knows", morpheus)
	if err := g.SaveAll(neo, morpheus, rel); err != nil {
		t.Fatalf("error saving all: %v", err)
	}

	if relations, err := g.Walk(neo, "knows"); err != nil {
		t.Errorf("error walking knows relation from neo. %v", err)
	} else {
		if len(relations) != 1 {
			t.Errorf("expecting one relation but got %v", len(relations))
		}
		for _, v := range relations {
			if !v.From.Is(neo) {
				t.Errorf("relation should be from neo to morpheus. but from is: %v", rel.From)
			}

			if !v.To.Is(morpheus) {
				t.Errorf("relation should be to morpheus but to is: %v", v.To)
			}
		}
	}
}

func BenchmarkSingleNodeInsert(b *testing.B) {
	g := mustOpenGraph(b)

	b.ResetTimer()
	prefix := fmt.Sprintf("%v", time.Now().UnixNano())
	for i := 0; i < b.N; i++ {
		node := &Node{
			Name: fmt.Sprintf("bench-%v-%v", prefix, i),
		}
		if err := g.SaveAll(node); err != nil {
			b.Fatalf("error saving node: %v", err)
		}
	}
}

func BenchmarkRelationInsert(b *testing.B) {
	g := mustOpenGraph(b)

	prefix := fmt.Sprintf("%v", time.Now().UnixNano())
	nodeA := &Node{
		Name: "nodeA-" + prefix,
	}
	nodeB := &Node{
		Name: "nodeB-" + prefix,
	}

	if err := g.SaveAll(nodeA, nodeB); err != nil {
		b.Fatalf("error saving nodes: %v", err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		relName := fmt.Sprintf("rel-%v-%v", prefix, i)
		if err := g.SaveAll(nodeA.Rel(relName, nodeB)); err != nil {
			b.Fatalf("error saving relation %v", err)
		}
	}
}
