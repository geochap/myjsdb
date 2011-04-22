 /*!
 * myjsdb
 * Copyright(c) 2011 geoff chappell <geoff@sover.net>
 * MIT Licensed
 */

var Client = require('mysql').Client;

var exports = module.exports;

var Store = exports.Store = function(name, options){
	this.name = name;
    this.options = options || {};
	
	this.client = new Client();
    for (var key in options)
        this.client[key] = options[key];

	this.open = function(fn){
		this.client.connect(fn);
	}

	this.close = function(fn){
		this.client.end(fn);
	}

    this.getDocument = function(id){
        return new Document(this, id);
    }

    this.query = function(desc, fn){
        var query = new QueryCompiler({name:this.name, store:this}).compile(desc);
//        console.log(query.getSql()); 
		this.client.query(query.getSql(), function(err, res){
			if (err) 
				return fn(err);			
			fn(err, new Sql2JsonHelper(res).getResults(desc));
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

	this.clearDocument = function(id, fn){
		this.client.query('delete from ' + this.name + '_json where docid=?',
			[id], fn
		);
	}

	this.putDocument = function(name, obj, fn){
		var store = this;

		var sm = {
			s1: function(){
				store.client.query('insert into ' + store.name + '_json_doc (name, last_modified)' +
					" values(?, now())" +
					" on duplicate key update last_modified = now()", [name], function(err, info){
						if (err) 
							return fn(err);
						if ((sm.docid = info.insertId) == 0)
							sm.s2();	
						else
							sm.s3();
					}
				);
			},				

			s2: function() {
				store.getDocumentId(name, function(err, id){ 
					if (err) 
						return fn(err);
					sm.docid = id;
					sm.s3();
				});
			},

			s3: function() {
				store.clearDocument(sm.docid, function(err){
					if (err) 
						return fn(err);
					sm.s4();
				});
			},

			s4: function() {
				var stmts = new Json2SqlHelper(store, sm.docid, obj).getStatements();
				store.client.query('insert into ' + store.name + '_json values ' + stmts.join(', '), function(err){
					fn(err);
				});
			}
		};

		sm.s1();	
	}

    this.create = function(fn){
		this.executeStatements([
			'create table ' + this.name + '_json_doc(id int auto_increment, name varchar(256), last_modified datetime,' +
				'primary key(id)) engine=InnoDB;',
			
			'create unique index ' + this.name + '_json_doc_ndx1 on ' + this.name + '_json_doc (name);',

			'create table ' + this.name + '_json(docid int, oid int, prop varchar(256), id_value int,' +
				'string_value varchar(8192), number_value float, bool_value bit, is_null bit,' +
				'primary key(docid, oid, prop)) engine=InnoDB;'
			], fn
		);
	}

	this.remove = function(fn){
		this.executeStatements([		
			'drop table ' + this.name + '_json_doc',
			'drop table ' + this.name + '_json;'
			], fn
		);
	}

	//a helper since the node mysql driver I'm using doesn't support multiple commands yet
	this.executeStatements = function(stmts, fn){
		var self = this;
		var stmt = stmts.shift();
		if (stmt){
//			console.log(stmt);
			this.client.query(stmt, function(err){
//				if (!checkError(err, fn))
					self.executeStatements(stmts, fn);
			});
		}
		else
			fn();
	}

	var checkError = function(err, fn){
		if (err){
			if (fn) fn(err);
			else throw err;
		}
		return err;
	}
}

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

		if (typeof o == 'array')
			return handleArray(id, o);

		for (var key in o)
			stmts.push(getStatement(id, o, key));
	
		return id;
	}

	var handleArray = function(id, a){
		return id;
	}

	var getStatement = function(id, o, prop){
		var val = o[prop];
		
		if (val == null)
			return '(' +[docid, id, wrap(prop), 'null', 'null', 'null', 'null', 1].join(',') + ')';

		switch (typeof val){
			case 'string':
				return '(' +[docid, id, wrap(prop), 'null', wrap(val), 'null', 'null', 0].join(',') + ')';
			case 'number':
				return '(' +[docid, id, wrap(prop), 'null', 'null', val, 'null', 0].join(',') + ')';
			case 'boolean':
				return '(' +[docid, id, wrap(prop), 'null', 'null', 'null', val?1:0, 0].join(',') + ')';
			case 'object':
				return '(' +[docid, id, wrap(prop), handleObject(val), 'null', 'null', 'null', 0].join(',') + ')';
			case 'array':
				return '(' +[docid, id, wrap(prop), handleObject(val), 'null', 'null', 'null', 0].join(',') + ')';
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

	var process = function(){
		for(var key in res)
			processRow(res[key]);
	}

	var processRow = function(row){
		var key = row.docid + '.' + row.oid;

		if (!self.objects[key])
			self.objects[key] = {};

		self.objects[key][row.prop] = getVal(row);

		if (!self.solutions[row.reskey])
			self.solutions[row.reskey] = new Solution();
		
		self.solutions[row.reskey].addObject(row.objref, self.objects[key]);
	}
	
	var getVal = function(row){
		if (row.string_value != null)
			return row.string_value;
		if (row.number_value != null)
			return row.number_value;
		if (row.bool_value != null)
			return row.bool_value?true:false;
		return null;
	}

	this.getResults = function(tmpl){
		process();

		var rows = [];

		for (var i in this.solutions)
			rows.push(this.solutions[i].applyTemplate(tmpl));

		return rows;
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
			var objref = this.objects[obj.objref.uniqueId()];
			if (objref != null && objref[obj.prop])
				return objref[obj.prop];
			return obj.value;
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

var ObjectRef = function(doc, desc){
    Constraints.call(this);

    this.document = doc;
    this.id = -1;

    if (typeof desc == 'object'){
        for (var key in desc)
            this[key] = new PropVal(this, key, desc[key]);
    }
    else if (typeof desc == 'number')
        this.id = desc;

    this.template = function(tmpl){
        for (var key in this)
            if (typeof tmpl[key] == 'undefined' && this[key] instanceof KeyVal)
                key.isOutput = false;
    }
}

var PropVal = function(objref, name, val){
    Constraints.call(this);

    this.objref = objref;
    this.name = name;
    this.value = val;
    this.isOutput = true;
}

var Constraints = function(){
    this.constraints = [];
    
    this.eq = function(o){
        this.constraints.push({eq: o});
        return o;
    }

    this.gt = function(o){
        this.constraints.push({gt: o});
        return o;
    }
}

var Query = function(options){
	var objrefs = {};
    var constraints = [];
    var conditions = [];
    var tables = [];
    var tablebydoc = {};
    var tablebyobj = {};
	var tablebyid = {};
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

		var vars = getSelect();

		var inner =  'select ' + vars.join(',') + ' from ' + tables.join(', ')
            + ' where ' + conditions.join(' and ');

		var selects = [];
		for (var i=0; i<vars.length-1; i+=2)
			selects.push('select a.*, b.reskey, \'' + outputs[i/2].uniqueId() + '\' as objref from ' + options.name + '_json a, (' + inner + ') b where '
				+ 'a.docid = b.v' + i + ' and a.oid = b.v' + (i+1));

		return selects.join(' union ');
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
    }

    var handleConstraint= function(constraint){
        var objref = constraint.objref,
            doc = objref.document,
            id = constraint.uniqueId(),
            docid = doc.uniqueId(),
            objid = objref.uniqueId(),
            pv = constraint.pv;

        if (!pv.isOutput && pv.value == null)
            return;

        var table = 't' + id;

		if (!tablebyid[id]){
	        tables.push(options.name + '_json ' + table);
			tablebyid[id] = table;
		}

        if (!tablebydoc[docid]){
            tablebydoc[docid] = table;
            if (doc.name){
                var doctable = 'td' + docid;
                tables.push(options.name + '_json_doc ' + doctable);

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

        if (typeof pv.value != 'function'){
			if (pv.value == null)
				conditions.push(table + '.is_null=1');
			else {
				switch (typeof(pv.value)){
					case 'string':
						conditions.push(table + '.string_value=' + options.store.client.escape(pv.value));
						break;
					case 'number':
						conditions.push(table + '.number_value=' + pv.value);
						break;
					case 'boolean':
						conditions.push(table + '.bool_value=' + pv.value?1:0);
						break;
				}
			}
        }

        conditions.push(table + '.prop=\'' + pv.name + '\'');
    }
}

var QueryCompiler = function(options){
	this.options = options || {};
	
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
			vos[obj.objref.uniqueId()] = true;

        if (obj == null || typeof obj != 'object' || sofar[obj.uniqueId()])
            return sofar;

        sofar[obj.uniqueId()] = obj;
        for (var key in obj)
            getRefs(obj[key], sofar, vos, visible && !(obj instanceof PropVal));

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


