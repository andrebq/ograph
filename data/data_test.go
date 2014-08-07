package data

import (
	"reflect"
	"testing"
	"time"
)

func mustCreateRepo(t *testing.T) *Repo {
	repo := Repo{}
	if err := repo.Connect("ograph", "ograph", "ograph", "localhost"); err != nil {
		t.Fatalf("unable to connect: %v", err)
	}

	if err := repo.Drop(); err != nil {
		t.Fatalf("unable to drop to a clean state. %v", err)
	}

	if err := repo.Create(); err != nil {
		t.Fatalf("unable to create the tables. %v", err)
	}
	return &repo
}

func TestInsertNode(t *testing.T) {
	repo := mustCreateRepo(t)
	defer repo.Close()
	nodeA := Node{Name: "node-a" + time.Now().String()}
	repo.Begin()
	if err := repo.SaveNode(&nodeA); err != nil {
		t.Fatalf("unable to save node-a. %v", err)
	}
	repo.End()

	if nodeA.Gid == InvalidGid {
		t.Fatalf("SaveNode should give a valid gid.")
	}

	nodeAFetch := Node{}
	if err := repo.FetchNode(nodeA.Name, InvalidGid, &nodeAFetch); err != nil {
		t.Fatalf("unable to fetch node-a. %v", err)
	}

	if !reflect.DeepEqual(nodeA, nodeAFetch) {
		t.Fatalf("nodes are distinct. expecting %v got %v", nodeA, nodeAFetch)
	}
}

func TestKeyword(t *testing.T) {
	repo := mustCreateRepo(t)
	defer repo.Close()
	kw := Keyword{Name: "a/key/word"}
	repo.Begin()
	if err := repo.SaveKeyword(&kw); err != nil {
		t.Fatalf("error saving keyword: %v", err)
	}
	repo.End()

	if kw.Gid == InvalidKid {
		t.Fatalf("save keyword should generate a valid id")
	}

	same := Keyword{Name: kw.Name}
	if err := repo.SaveKeyword(&same); err != nil {
		t.Fatalf("error saving the same keyword: %v", err)
	}

	if !reflect.DeepEqual(same, kw) {
		t.Fatalf("when saving the same keyword, they should be the same. expecting %v got %v", kw, same)
	}

	fetch := Keyword{}
	if err := repo.Keyword(kw.Name, &fetch); err != nil {
		t.Fatalf("error while fetching keyword by name. %v", err)
	}
	if !reflect.DeepEqual(fetch, kw) {
		t.Fatalf("keyword fetched by name != from original. expecting %v got %v", kw, fetch)
	}

	fetch = Keyword{}
	if err := repo.Keyword(kw.Gid, &fetch); err != nil {
		t.Fatalf("error while fecthing keyword by gid. %v", err)
	}
	if !reflect.DeepEqual(kw, fetch) {
		t.Fatalf("keyword fetched by id != form original. expecting %v got %v", kw, fetch)
	}
}

func TestInsertRelation(t *testing.T) {
	repo := mustCreateRepo(t)
	defer repo.Close()
	neo := Node{Name: "neo-" + time.Now().String()}
	morpheus := Node{Name: "morpheus" + time.Now().String()}
	operator := Node{Name: "operator" + time.Now().String()}
	repo.Begin()
	repo.SaveNode(&neo)
	repo.SaveNode(&morpheus)
	repo.SaveNode(&operator)
	repo.End()
	if repo.Err() != nil {
		t.Fatalf("error saving nodes: %v", repo.Err())
	}

	relation := Relation{}
	relation.Set(&neo, "knows", &morpheus)

	_ = relation
	repo.Begin()
	if err := repo.SaveRelation(&relation); err != nil {
		t.Fatalf("error saving relation: %v", err)
	}
	repo.End()

	repo.Begin()
	operatorRel := Relation{}
	operatorRel.Set(&neo, "knows", &operator)
	if err := repo.SaveRelation(&operatorRel); err != nil {
		t.Fatalf("error saving relation: %v", err)
	}
	repo.End()

	fetch := Relation{}
	if err := repo.FetchRelation(neo.Gid, morpheus.Gid, "knows", &fetch); err != nil {
		t.Fatalf("error fetching relation: %v", err)
	}

	if !reflect.DeepEqual(relation, fetch) {
		t.Fatalf("fetched relation should be equal to the initial. expecting %v got %v", relation, fetch)
	}

	// now, let's test the walk

	if set, err := repo.Walk(neo.Gid, "knows", nil); err != nil {
		t.Fatalf("error walking knows relation from neo. %v", err)
	} else {
		if len(set) == 0 {
			t.Fatalf("should have found at least one relation with morpheus")
		}
		for _, rel := range set {
			if !reflect.DeepEqual(rel, &relation) &&
				!reflect.DeepEqual(rel, &operatorRel) {
				t.Errorf("unexpected relation in set %v", rel)
			}
		}
	}
}
