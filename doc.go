// ograph is a graph database that uses postgresql as the backend store.
//
// each node have a list of attributes (encoded as JSON), a unique name, and a list of relations
//
// relations can also have attributes, all relations are directed, have a name and two endpoints.
package ograph
