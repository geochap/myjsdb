# myjsdb

## Purpose

A node.js module that turns MySQL into a JSON store.

## Current status

Only basic functionality so far -- you can put documents and perform basic querying.

## Contributors

* Geoff Chappell ([geochap](https://github.com/geochap)) - Author and maintainer

##Installation

	cd ~/.node_libraries
	git clone git://github.com/geochap/myjsdb.git myjsdb

## Tutorial

    var Store = require('myjsdb').Store;

    var store = new Store('test', {user:'root', password:'root', database:'testdb'}),
        doc = store.getDocument(),
        person = doc.getObject({age:Number, name:'Geoff'});

    person.age.gt(10);

    //we can be lazy and rely on the queuing effect of the underlying mysql module
    //so we can skip the callbacks in most cases and not chain the calls
    store.open();

    //store.create();

    store.putDocument('test', {name:'geoff', age:44, knows:{name:'derrish'}});

    store.query({age:person.age, name:person.name}, function(err, res){
        console.log(res);
    });

    //store.remove();

    store.close();

