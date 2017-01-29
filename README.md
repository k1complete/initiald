InitialD
========

[![Build Status](https://travis-ci.org/k1complete/initiald.svg?branch=master)](https://travis-ci.org/k1complete/initiald)

# Real relational database language

InitialD is a relational database language inspired by the tutorial D.

# Datatypes

## Reltype

It is name and definition pair.  name is atom,
definition is (any -> boolean) function.  Reltype is
stored into :reltype mnesia table.

## Relval

It is mnesia table, list of tuple and query of there.
tuple format is {table, key, att1, att2, att3, ...}.
key is a tuple of primary key of attributes or a
attribute.  Relval expressions are closure by
itself(except relational assignment).
Relval has name, types, query and keys.

## Relvar

It is mnesia table.  It is variable.
Relval has name, types, query and keys.

## Reltuple

It is virtual structure for Relval literal.
Reltuple has name, types, and body.

## Constraint


