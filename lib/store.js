/*!
 * myjsdb
 * Copyright(c) 2011 geoff chappell <geoff@sover.net>
 * MIT Licensed
 */

var Client = require('mysql').Client;

var exports = module.exports;

var Store = exports.Store = function(name, options){
    this.name = name;
    this.client = new Client(options || {});

    this.open = function(fn){
        this.client.connect(fn);
    }

    this.close = function(fn){
        this.client.end(fn);
    }

    this.getDocumentRef = function(id){
        return new Document(this, id);
    }

    this.beginTransaction = function(fn){
        this.client.query('begin transaction', fn);
    }

    this.endTransaction = function(commit, fn){
        this.client.query(commit?'commit transaction':'rollback transaction', fn);
    }

    this.query = function(desc, options, fn){
        if (typeof options == 'function' && fn == null){
            fn = options;
            options = null;
        }

        options = options || {};
        options.store = this;

        var query = new QueryCompiler(options).compile(desc);
//        console.log(query.getSql());
        this.client.query(query.getSql(), function(err, res){
            fn(err, err?null:new Sql2JsonHelper(res).getResults(desc));
        });
    }

    this.getDocumentId = function(name, fn){
        this.client.query('select id from ' + this.name + '_json_doc where name=?',
            [name],
            function(err, res){
                fn(err, res?res[0].id:null);
            }
        );
    }

    this.clearDocumentById = function(id, fn){
        this.client.query('delete from ' + this.name + '_json where docid=?',
            [id], fn);
    }

    this.putDocument = function(name, obj, fn){
        var store = this;
        var docid = 0;

        var s1 = function(){
            store.client.query('insert into ' + store.name + '_json_doc (name, last_modified)' +
                " values(?, now())" +
                " on duplicate key update last_modified = now()", [name], function(err, info){
                    if (err)
                        return fn(err);
                    if ((docid = info.insertId) == 0)
                        s2();
                    else
                        s3();
                }
            );
        };

        var s2 = function() {
            store.getDocumentId(name, function(err, id){
                if (err)
                    return fn(err);
                docid = id;
                s3();
            });
        };

        var s3 = function() {
            store.clearDocumentById(docid, function(err){
                if (err)
                    return fn(err);
                s4();
            });
        };

        var s4 = function() {
            var stmts = new Json2SqlHelper(store, docid, obj).getStatements();
            store.client.query('insert into ' + store.name + '_json values ' + stmts.join(', '), function(err){
                fn(err);
            });
        };

        s1();
    }

    this.getDocument = function(name, fn){
        this.client.query('select a.* from ' + this.name + '_json a join ' + this.name + '_json_doc b on a.docid = b.id'
            + ' where b.name=?', [name], function(err, res){
                fn(err, err?null:new Sql2JsonHelper(res).getRoot());
            });
    }

    this.create = function(fn){
        this.executeStatements([
            'create table ' + this.name + '_json_doc(id int auto_increment, name varchar(256), last_modified datetime,' +
                'primary key(id)) engine=InnoDB;',

            'create unique index ' + this.name + '_json_doc_ndx1 on ' + this.name + '_json_doc (name);',

            'create table ' + this.name + '_json(docid int, oid int, prop varchar(256), id_value int,' +
                'string_value varchar(8192), number_value float, bool_value bit, flag int,' +
                'primary key(docid, oid, prop)) engine=InnoDB;',

            'create index ' + this.name + '_json_ndx1 on ' + this.name + '_json(prop, id_value, oid, docid);',
            'create index ' + this.name + '_json_ndx2 on ' + this.name + '_json(prop, string_value, oid, docid);',
            'create index ' + this.name + '_json_ndx3 on ' + this.name + '_json(prop, number_value, oid, docid);',
            'create index ' + this.name + '_json_ndx4 on ' + this.name + '_json(prop, bool_value, oid, docid);'
            ], fn
        );
    }

    this.remove = function(fn){
        this.executeStatements([
            'drop table ' + this.name + '_json_doc;',
            'drop table ' + this.name + '_json;'
            ], fn
        );
    }

    this.executeStatements = function(stmts, fn){
        var self = this;
        var stmt = stmts.shift();
        if (stmt){
            this.client.query(stmt, function(err){
                if (err){
                    if (fn) fn(err); else throw err;
                }
                self.executeStatements(stmts, fn);
            });
        }
        else if (fn) fn();
    }
}

Store.FLAG_NON_NULL_PROP_VAL = 0;
Store.FLAG_NULL_PROP_VAL = 1;
Store.FLAG_OBJECT = 2;
Store.FLAG_ARRAY = 4;
Store.FLAG_ROOT = 8;


var Json2SqlHelper = function(store, docid, obj){
    var stmts = [];
    var oids = {};
    var nextid = 1;

    this.getStatements = function(){
        handleObject(obj);

        return stmts;
    }

    var handleObject = function(o){
        if (oids[o.uniqueId()])
            return oids[o.uniqueId()];

        var id = oids[o.uniqueId()] = nextid++;
        if (o instanceof Array)
            return handleArray(id, o);

        stmts.push('(' +[docid, id, wrap(''), 'null', 'null', 'null', 'null', Store.FLAG_OBJECT + (o == obj?Store.FLAG_ROOT:0)].join(',') + ')');
        for (var key in o)
            stmts.push(getStatement(id, o, key));

        return id;
    }

    var handleArray = function(id, a){
        stmts.push('(' +[docid, id, wrap(''), 'null', 'null', 'null', 'null', Store.FLAG_ARRAY + (a == obj?Store.FLAG_ROOT:0)].join(',') + ')');

        for (var i in a)
            stmts.push(getStatement(id, a, i));

        return id;
    }

    var getStatement = function(id, o, prop){
        var val = o[prop];

        if (val == null)
            return '(' +[docid, id, wrap(prop), 'null', 'null', 'null', 'null', Store.FLAG_NULL_PROP_VAL].join(',') + ')';

        switch (typeof val){
            case 'string':
                return '(' +[docid, id, wrap(prop), 'null', wrap(val), 'null', 'null', Store.FLAG_NON_NULL_PROP_VAL].join(',') + ')';
            case 'number':
                return '(' +[docid, id, wrap(prop), 'null', 'null', val, 'null', Store.FLAG_NON_NULL_PROP_VAL].join(',') + ')';
            case 'boolean':
                return '(' +[docid, id, wrap(prop), 'null', 'null', 'null', val?1:0, Store.FLAG_NON_NULL_PROP_VAL].join(',') + ')';
            case 'object':
                return '(' +[docid, id, wrap(prop), handleObject(val), 'null', 'null', 'null', Store.FLAG_NON_NULL_PROP_VAL].join(',') + ')';
        }
    }

    var wrap = function(s){
        return store.client.escape(s);
    }
}

var Sql2JsonHelper = function(res){
    this.objects = {};
    this.solutions = {};
    var self = this;
    var root = null;

    var process = function(){
        for (var key in res)
            preProcessRow(res[key]);

        for(var key in res)
            processRow(res[key]);
    }

    var preProcessRow = function(row){
        var key = row.docid + '.' + row.oid;

        if (row.flag & Store.FLAG_OBJECT)
            self.objects[key] = {};
        else if (row.flag & Store.FLAG_ARRAY)
            self.objects[key] = [];
        if (row.flag & Store.FLAG_ROOT)
            root = self.objects[key];
    }

    var processRow = function(row){
        var key = row.docid + '.' + row.oid;

        if (row.prop != '')
            self.objects[key][row.prop] = getVal(row);
        if (row.reskey){
            if (!self.solutions[row.reskey])
                self.solutions[row.reskey] = new Solution();

            self.solutions[row.reskey].addObject(row.objref, self.objects[key]);
        }
    }

    var getVal = function(row){
        if (row.string_value != null)
            return row.string_value;
        if (row.number_value != null)
            return row.number_value;
        if (row.bool_value != null)
            return row.bool_value?true:false;
        if (row.id_value != null)
            return self.objects[row.docid + '.' + row.id_value];

        return null;
    }

    this.getResults = function(tmpl){
        process();

        var rows = [];

        for (var i in this.solutions)
            rows.push(this.solutions[i].applyTemplate(tmpl));

        return rows;
    }

    this.getRoot = function(){
        process();

        return root;
    }
}

var Solution = function(){
    this.objects = {};

    this.addObject = function(ref, obj){
        this.objects[ref] = obj;
    }

    this.applyTemplate = function(tmpl){
        return this.getValue(tmpl);
    }

    this.getValue = function(obj){
        if (obj instanceof ObjectRef)
            return this.objects[obj.uniqueId()];

        if (obj instanceof PropVal){
            if (obj.__value instanceof ObjectRef)
                return this.objects[obj.__value.uniqueId()];
            var objref = this.objects[obj.__objref.uniqueId()];
            if (objref != null && objref[obj.__name])
                return objref[obj.__name];
            return obj.__value;
        }

        if (obj && typeof obj == 'object'){
            var res = {};
            for (var key in obj)
                res[key] = this.getValue(obj[key]);
            return res;
        }

        return obj;
    }
}

// a document holds a single root json element (either {} or [])
// document can be initialized with null id (i.e. any document),
// with a particular id, or with a name
var Document = function(store, id){
    this.store = store;
    this.id = null;
    this.name = null;

    if (typeof id == 'number')
        this.id = id;
    else if (typeof id == 'string')
        this.name = id;

    this.getObject = function(desc){
        return new ObjectRef(this, desc);
    }
}

// a ObjectRef refers to some object {} or [] in a document
var ObjectRef = function(doc, desc){
    Constraints.call(this);

    this.document = doc;
    this.id = -1;
    this.isArray = false;

    if (typeof desc == 'object'){
        for (var key in desc)
            this[key] = new PropVal(this, key, desc[key]);
        this.isArray = desc instanceof Array;
    }
    else if (typeof desc == 'number')
        this.id = desc;
}

// a PropVal is a property and value on an object
// the value may be a type - e.g. String, Number
// when a property is just declared for later use
// in a constraint or in an output template
var PropVal = function(objref, name, val){
    Constraints.call(this);

    this.__objref = objref;
    this.__name = name;
    this.__value = val;

    if (typeof val == 'object'){
        this.__value = new ObjectRef(objref.document, val);
        for (var key in val)
            this[key] = this.__value[key];
    }
}

// a base class for things that can be constrained (ObjectRef and PropVal)
var Constraints = function(){
    this.constraints = [];

    this.eq = function(o){
        this.constraints.push({type:'eq', arg1:this, arg2:o});
        return this;
    }

    this.ne = function(o){
        this.constraints.push({type:'ne', arg1:this, arg2:o});
        return this;
    }

    this.gt = function(o){
        this.constraints.push({type:'gt', arg1:this, arg2:o});
        return this;
    }

    this.gte = function(o){
        this.constraints.push({type:'gte', arg1:this, arg2:o});
        return this;
    }

    this.lt = function(o){
        this.constraints.push({type:'lt', arg1:this, arg2:o});
        return this;
    }

    this.lte = function(o){
        this.constraints.push({type:'lte', arg1:this, arg2:o});
        return this;
    }
}

// the representation of a query
var Query = function(options){
    var objrefs = {};
    var constraints = [];
    var conditions = [];
    var tables = [];
    var tablebydoc = {};
    var tablebyobj = {};
    var tablebyid = {};
    var tablebypvid = {};
    var outputs = [];

    this.addObjRef = function(objref, output){
        objrefs[objref.uniqueId()] = objref;
        if (output)
            outputs.push(objref);
    }

    this.getSql = function(){
        for (var key in objrefs)
            addConstraints(objrefs[key]);

        for (key in constraints)
            handleConstraint(constraints[key]);

        for (key in constraints)
            handleOperators(constraints[key]);

        var vars = getSelect();

        var inner =  'select ' + vars.join(',') + ' from ' + tables.join(', ')
            + ' where ' + conditions.join(' and ');

        if (typeof options.orderby != 'undefined')
            inner += getOrderBy(options.orderby);

        if (typeof options.limit == 'number')
            inner += ' limit ' + options.limit;

        if (typeof options.offset == 'number'){
            if (typeof options.limit != 'number')
                inner += ' limit 18446744073709551615';
            inner += ' offset ' + options.offset;
        }

        var selects = [];
        for (var i=0; i<vars.length-1; i+=2)
            selects.push('select a.*, b.reskey, \'' + outputs[i/2].uniqueId() + '\' as objref from ' + options.store.name + '_json a, (' + inner + ') b where '
                + 'a.docid = b.v' + i + ' and a.oid = b.v' + (i+1));

        return selects.join(' union ');
    }

    var getOrderBy = function(desc){
        if (!(desc instanceof Array))
            desc = [desc];

        var orderbys = [];
        for (var key in desc){
            var o = desc[key];
            if (o instanceof PropVal)
                o = {asc:o};

            var dir = o.asc == null?'desc':'asc';

            var col = o[dir];
            if (!(col instanceof PropVal))
                throw new Error('invalid order by');

            var table = tablebypvid[col.uniqueId()];
            if (table == null)
                throw new Error('invalid order by');

            orderbys.push(table + getColumn(col) + ' ' + dir);
        }
        return ' order by ' + orderbys.join(', ');
    }

    var getSelect = function(){
        var vars = [];
        var aliases = [];

        for (var key in outputs){
            var table = tablebyobj[outputs[key].uniqueId()];

            vars.push(table + '.docid as v' + vars.length);
            vars.push(table + '.oid as v' + vars.length);

            aliases.push(table + '.docid');
            aliases.push(table + '.oid');
        }

        vars.push('concat(' + aliases.join(',') + ') as reskey');

        return vars;
    }

    // add constraints for an propvals on object refs in the query
    var addConstraints = function(objref){
        for (var prop in objref){
            var pv = objref[prop];
            if (pv instanceof PropVal){
                constraints.push({
                    objref: objref,
                    pv: pv
                });
            }
        }

        for (var c in objref.constraints){
        }
    }

    //turn the constraints into sql equivalents
    var handleConstraint= function(constraint){
        var objref = constraint.objref,
            doc = objref.document,
            id = constraint.uniqueId(),
            docid = doc.uniqueId(),
            objid = objref.uniqueId(),
            pv = constraint.pv;

        if (pv.__value == null)
            return;

        var table = 't' + id;

        tablebypvid[pv.uniqueId()] = table;

        if (!tablebyid[id]){
            tables.push(options.store.name + '_json ' + table);
            tablebyid[id] = table;
        }

        if (!tablebydoc[docid]){
            tablebydoc[docid] = table;
            if (doc.name){
                var doctable = 'td' + docid;
                tables.push(options.store.name + '_json_doc ' + doctable);

                conditions.push(table + '.docid=' + doctable + '.id');
                conditions.push(doctable + '.name=\'' + doc.name + '\'');
            }
            else if (doc.id){
                conditions.push(table + '.docid=' + doc.id);
            }
        }
        else
            conditions.push(table + '.docid=' + tablebydoc[docid] + '.docid');

        if (!tablebyobj[objid])
            tablebyobj[objid] = table;
        else
            conditions.push(table + '.oid=' + tablebyobj[objid] + '.oid');

        if (typeof pv.__value != 'function'){
            if (pv.__value == null)
                conditions.push(table + '.flag=' + Store.FLAG_NULL_PROP_VAL);
            else {
                switch (typeof(pv.__value)){
                    case 'string':
                        conditions.push(table + '.string_value=' + options.store.client.escape(pv.__value));
                        break;
                    case 'number':
                        conditions.push(table + '.number_value=' + pv.__value);
                        break;
                    case 'boolean':
                        conditions.push(table + '.bool_value=' + pv.__value?1:0);
                        break;
                }
            }
        }

        conditions.push(table + '.prop=\'' + pv.__name + '\'');
    }

    var handleOperators = function(constraint){
        var pv = constraint.pv;
        var table = 't' + constraint.uniqueId();
        for (var cons in pv.constraints){
            conditions.push(table + getConstraint(pv, pv.constraints[cons]))
        }
    }

    var getConstraint = function(pv, constraint){
        var op;

        switch(constraint.type){
            case 'eq': op = '='; break;
            case 'ne': op = '<>'; break;
            case 'lt': op = '<'; break;
            case 'lte': op = '<='; break;
            case 'gt': op = '>'; break;
            case 'gte': op = '>='; break;
            default: throw new Error('unknown operator: ' + constraint.type);
        }

        return getColumn(pv, constraint) + op +  getValue(pv, constraint);
    }

    var getColumn = function(pv, constraint){
        if ((constraint != null && typeof constraint.arg2 == 'string') || pv.__value == String)
            return ".string_value";
        if ((constraint != null && typeof constraint.arg2 == 'number') || pv.__value == Number)
            return ".number_value";
        if ((constraint != null && typeof constraint.arg2 == "boolean") || pv.__value == Boolean)
            return ".bool_value";

        return ".id_value";
    }


    var getValue = function(pv, constraint){
        if (constraint.arg2 instanceof PropVal)
            return 't' + constraint.arg2.uniqueId() + getColumn(constraint.arg2, constraint);
        else if (constraint.arg2 instanceof ObjectRef){
            var t = tablebyobj[constraint.arg2.uniqueId()];
            if (t)
                return t + '.oid';
            throw new Error('invalid query');
        }
        return options.store.client.escape(constraint.arg2);
    }
}

var QueryCompiler = function(options){

    this.compile = function(desc){
        var query = new Query(options);
        var vos = {};

        var refs = getRefs(desc, {}, vos, true);

        for (var key in refs){
            var objref = refs[key];
            if (objref instanceof ObjectRef)
                query.addObjRef(objref, vos[objref.uniqueId()]);
        }

        return query;
    }

    var getRefs = function(obj, sofar, vos, visible){
        if (visible && obj instanceof ObjectRef)
            vos[obj.uniqueId()] = true;
        if (visible && obj instanceof PropVal)
            vos[obj.__objref.uniqueId()] = true;

        if (obj == null || typeof obj != 'object' || sofar[obj.uniqueId()])
            return sofar;

        sofar[obj.uniqueId()] = obj;
        for (var key in obj)
            getRefs(obj[key], sofar, vos, visible && key != 'constraints');

        return sofar;
    }
}

ObjectRef.prototype.__proto__ = Constraints.prototype;
PropVal.prototype.__proto__ = Constraints.prototype;

(function() {
    if ( typeof Object.prototype.uniqueId == "undefined" ) {
        var id = 0;
    Object.defineProperty(Object.prototype, 'uniqueId', {
            enumerable:false,
            value:function(){
                if ( typeof this.__uniqueid == "undefined" ) {
                    this.__uniqueid = ++id;
                    Object.defineProperty(this, '__uniqueid', {enumerable:false});
                }
                return this.__uniqueid;
            },
            configurable:true
        });
    }
})();


