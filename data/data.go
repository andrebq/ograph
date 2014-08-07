package data

import (
	"database/sql"
	"errors"
	_ "github.com/lib/pq"
	"fmt"
)

type (
	Node struct {
		Gid uint64
		Name string
		Attributes string
	}

	Relation struct {
		FromGid uint64
		FromName string
		FromAttributes string

		ToGid uint64
		ToName string
		ToAttributes string

		Attributes string
		Field uint32
		Name string
	}

	Keyword struct {
		Gid uint32
		Name string
	}

	Repo struct {
		Db Db
		Transaction Transaction
		err error
		AutoCommit bool
	}

	Querier interface {
		QueryRow(query string, args ...interface{}) *sql.Row
		Query(query string, args ...interface{}) (*sql.Rows, error)
		Exec(query string, args ...interface{}) (sql.Result, error)
	}

	Db interface {
		Querier
		Begin() (Transaction, error)
		Close() error
	}

	Transaction interface {
		Querier
		Rollback() error
		Commit() error
	}

	RelationSet []*Relation

	dbWrap struct {
		sql.DB
	}

	scanner interface {
		Scan(args ...interface{}) error
	}
)

func (d *dbWrap) Begin() (Transaction, error) {
	return d.DB.Begin()
}

var (
	sqlCreateTables = []string{
		`create table if not exists nodes (gid bigserial,
			name text not null constraint unq_name_cannot_repeat unique,
			attributes json, primary key (gid))`,
		`create table if not exists relations (field int not null, attributes json, from_ bigint not null, to_ bigint not null, primary key (from_, to_, field),
		foreign key(from_) references nodes(gid),
		foreign key(to_) references nodes(gid))`,
		`create table if not exists keywords ( kid serial primary key, name text not null)`,
	}

	sqlDrop = []string{
		`drop table if exists relations`,
		`drop table if exists nodes`,
		`drop table if exists keywords`,
	}

	sqlDelete = []string {
		`delete from relations`,
		`delete from nodes`,
	}
)

const (
	selectKeywordByGid      = `select kid, name from keywords where kid = $1`
	selectKeywordByName = `select kid, name from keywords where name = $1`
	insertKeyword      = `insert into keywords(name) values ($1) returning kid`
	selectNodeByGid    = `select gid, name, attributes from nodes where gid = $1`
	selectNodeByNameEq = `select gid, name, attributes from nodes where name = $1`
	insertNode         = `insert into nodes(name, attributes) values ($1, $2) returning gid`
	updateNode         = `update nodes set attributes = $2 where gid = $1 returning name`
	insertRelation     = `insert into relations (from_, to_, field, attributes) values ($1, $2, $3, $4)`
	updateRelation     = `update relations set attributes = $4 where from_ = $1 and to_ = $2 and field = $3`
	selectRelation = `select f.gid, f.name, f.attributes,
		t.gid, t.name, t.attributes,
		r.field, kw.name, r.attributes
		from relations r
			inner join keywords kw
				on kw.kid = $3 and r.field = kw.kid
			inner join nodes f
				on f.gid = $1 and r.from_ = f.gid
			inner join nodes t
				on t.gid = $2 and r.to_ = t.gid`
	selectRelationWalk = `select f.gid, f.name, f.attributes,
		t.gid, t.name, t.attributes,
		r.field, kw.name, r.attributes
		from relations r
			inner join nodes f
				on f.gid = $1 and r.from_ = f.gid
			inner join keywords kw
				on kw.kid = $2 and r.field = kw.kid
			inner join nodes t
				on r.to_ = t.gid`

	InvalidGid = uint64(0)
	InvalidKid = uint32(0)
)

func (r *Relation) CopyFromData(n *Node) {
	r.FromGid = n.Gid
	r.FromName = n.Name
	r.FromAttributes = n.Attributes
}

func (r *Relation) CopyToData(n *Node) {
	r.ToGid = n.Gid
	r.ToName = n.Name
	r.ToAttributes = n.Attributes
}

func (r *Relation) Set(subject *Node, object string, predicate *Node) *Relation {
	r.CopyFromData(subject)
	r.Name = object
	r.CopyToData(predicate)
	return r
}
func (nr *Repo) Create() error {
	var firstError error
	for _, cmd := range sqlCreateTables {
		_, err := nr.Db.Exec(cmd)
		if err != nil {
			// continue but keep the first error
			firstError = err
		}
	}
	nr.err = firstError
	return nr.err
}

func (nr *Repo) Drop() error {
	var firstError error
	for _, cmd := range sqlDrop {
		_, err := nr.Db.Exec(cmd)
		if err != nil {
			// continue but keep the first error
			firstError = err
		}
	}
	if firstError == nil {
		firstError = nr.Create()
	}
	nr.err = firstError
	return nr.err
}

// DeleteAll remove all nodes and relations from the database but keep
// all keywords
func (nr *Repo) DeleteAll() error {
	var firstError error
	for _, cmd := range sqlDelete {
		_, err := nr.Db.Exec(cmd)
		if err != nil {
			// continue but keep the first error
			firstError = err
		}
	}
	if firstError == nil {
		firstError = nr.Create()
	}
	nr.err = firstError
	return nr.err
}

func (nr *Repo) Connect(user, password, dbname, host string) error {
	var sqldb *sql.DB
	sqldb, nr.err = sql.Open("postgres", fmt.Sprintf("user=%v dbname=%v password=%v host=%v sslmode=disable", user, dbname, password, host))
	nr.Db = &dbWrap{*sqldb}
	return nr.err
}

func (nr *Repo) FetchNode(name string, gid uint64, out *Node) error {
	querier := nr.ActiveQuerier()
	var err error
	if gid != 0 {
		err = querier.QueryRow(selectNodeByGid, gid).Scan(&out.Gid, &out.Name, &out.Attributes)
	} else {
		err = querier.QueryRow(selectNodeByNameEq, name).Scan(&out.Gid, &out.Name, &out.Attributes)
	}
	return err
}

func (nr *Repo) SaveNode(node *Node) error {
	if !nr.Begin() {
		return nr.err
	}
	if len(node.Attributes) == 0 {
		node.Attributes = "{}"
	}
	if node.Gid == 0 {
		// insert
		nr.err = nr.Transaction.QueryRow(insertNode, node.Name, node.Attributes).Scan(&node.Gid)
	} else {
		// update
		_, nr.err = nr.Transaction.Exec(updateNode, node.Gid, node.Attributes)
	}
	return nr.err
}

func (nr *Repo) Keyword(id interface{}, out *Keyword) error {
	if nr.err != nil {
		return nr.err
	}
	querier := nr.ActiveQuerier()
	switch id := id.(type) {
	case uint32:
		nr.err = querier.QueryRow(selectKeywordByGid, id).Scan(&out.Gid, &out.Name)
	case string:
		nr.err = querier.QueryRow(selectKeywordByName, id).Scan(&out.Gid, &out.Name)
	default:
		fmt.Errorf("cannot use %#v as keyword identification", id)
	}
	return nr.err
}

func (nr *Repo) SaveKeyword(kw *Keyword) error {
	if !nr.Begin() {
		return nr.err
	}
	if len(kw.Name) == 0 {
		nr.err = errors.New("cannot save an empty keyword")
		return nr.err
	}
	// try to check if the keyword already exists
	nr.err = nr.Transaction.QueryRow(selectKeywordByName, kw.Name).Scan(&kw.Gid, &kw.Name)
	if nr.err == sql.ErrNoRows {
		nr.err = nil
		nr.err = nr.Transaction.QueryRow(insertKeyword, kw.Name).Scan(&kw.Gid)
	}
	return nr.err
}

func (r *Repo) SaveRelation(rel *Relation) error {
	if rel.FromGid == InvalidGid {
		r.err = errors.New("from is required")
	}
	if rel.ToGid == InvalidGid {
		r.err = errors.New("to is required")
	}
	if !r.Begin() {
		return r.err
	}
	if len(rel.Attributes) == 0 {
		rel.Attributes = "{}"
	}

	// read the keyword
	var kw Keyword
	if r.err = r.Keyword(rel.Name, &kw); r.err != nil {
		if r.err == sql.ErrNoRows {
			// try to insert
			r.err = nil
			kw.Name = rel.Name
			r.err = r.SaveKeyword(&kw)
		}
	}
	if r.err != nil {
		// abort here
		return r.err
	}
	rel.Field = kw.Gid
	rel.Name = kw.Name
	// if we are here, kw holds the kid
	activeQuerier := r.ActiveQuerier()
	var result sql.Result
	if result, r.err = activeQuerier.Exec(updateRelation, rel.FromGid, rel.ToGid, rel.Field, rel.Attributes); r.err != nil {
		// abort here
		return r.err
	}
	var affected int64
	// check if the result means that no row was updated
	if affected, r.err = result.RowsAffected(); r.err != nil {
		// abort here
		return r.err
	}

	if affected > 0 {
		// done and okay
		return nil
	}
	// insert
	_, r.err = activeQuerier.Exec(insertRelation, rel.FromGid, rel.ToGid, rel.Field, rel.Attributes);
	return r.err
}

func (r *Repo) Walk(from uint64, name string, out RelationSet) (RelationSet, error) {
	if r.err != nil {
		return out, r.err
	}
	var kw Keyword
	if err := r.Keyword(name, &kw); err != nil {
		return nil, err
	}
	activeQuerier := r.ActiveQuerier()
	if out == nil {
		out = make(RelationSet, 0)
	}

	rows, err := activeQuerier.Query(selectRelationWalk, from, kw.Gid)
	if err != nil {
		r.err = err
		return out, err
	}
	for rows.Next() {
		var rel Relation
		r.err = scanRelation(rows, &rel)
		if r.err != nil {
			break
		}
		out.Push(&rel)
	}
	r.err = rows.Err()
	return out, r.err
}

func (r *Repo) FetchRelation(from, to uint64, name string, out *Relation) error {
	activeQuerier := r.ActiveQuerier()
	if r.err != nil {
		return r.err
	}

	var kw Keyword
	if err := r.Keyword(name, &kw); err != nil {
		return err
	}

	r.err = scanRelation(activeQuerier.QueryRow(selectRelation, from, to, kw.Gid), out)
	return r.err
}

func scanRelation(sc scanner, out *Relation) error {
	return sc.Scan(&out.FromGid, &out.FromName, &out.FromAttributes,
		&out.ToGid, &out.ToName, &out.ToAttributes,
		&out.Field, &out.Name, &out.Attributes)
}

func (r *Repo) Begin() bool {
	if r.err == nil && r.Transaction == nil {
		r.Transaction, r.err = r.Db.Begin()
	}
	return r.err == nil
}

func (r *Repo) End() error {
	if r.Transaction != nil {
		defer func() {
			r.Transaction = nil
		}()
		if r.err == nil {
			return r.Transaction.Commit()
		} else {
			return r.Transaction.Rollback()
		}
	}
	return nil
}

func (r *Repo) ActiveQuerier() Querier {
	if r.Transaction != nil {
		return r.Transaction
	}
	return r.Db
}

func (r *Repo) Err() error {
	return r.err
}

func (r *Repo) AbortPending() error {
	if r.Transaction != nil {
		err := r.Transaction.Rollback()
		if r.err == nil {
			r.err = err
		}
		return err
	}
	return nil
}

func (r *Repo) Close() error {
	r.err = r.AbortPending()
	err := r.Db.Close()
	if r.err == nil {
		r.err = nil
	}
	return err
}

func (r *RelationSet) Push(rel *Relation) {
	*r = append(*r, rel)
}
