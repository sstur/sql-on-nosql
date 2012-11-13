/**
 * SQL-inspired interface on top of NoSQL document storage
 *
 * Based on:
 * DOM Storage Query Language
 * http://code.google.com/p/dom-storage-query-language
 * Copyright (c) 2010-2011 Pete Boere - pete@the-echoplex.net
 *
 */
(function(exports) {
  "use strict";

  //this represents the two datastores in the browser
  var _datastores = {
    local: {tables: {}},
    session: {tables: {}}
  };

  // Helpers
  var forEach = function(obj, callback) {
    if (Array.isArray(obj)) {
      obj.forEach(callback);
    } else
    if (obj === Object(obj)) {
      var keys = Object.keys(obj);
      keys.forEach(function(key) {
        callback.call(obj, key, obj[key]);
      });
    }
  };
  var extractLiterals = function(str, subs) {
    var literals = {}
      , prefix = 'LIT'
      , counter = 0
      , label
      , m;
    while (m = /('|")(?:\\1|[^\1])*?\1/.exec(str)) {
      label = '_' + prefix + (++counter) + '_';
      literals[label] = m[0].substring(1, m[0].length-1);
      str = str.substring(0, m.index) + label + str.substring(m.index + m[0].length);
    }

    // Apply any substitutions
    if (subs) {
      var test = {}.toString.call(subs);
      if (test === '[object Object]') {
        forEach(subs, function(key, value) {
          if (str.indexOf(':' + key) !== -1) {
            var patt = new RegExp('\\:' + key + '\\b');
            label = '_' + prefix + (++counter) + '_';
            literals[label] = value;
            str = str.replace(patt, label);
          }
        });
      }
      else if (test === '[object Array]') {
        while (str.indexOf('?') !== -1) {
          label = '_' + prefix + (++counter) + '_';
          literals[label] = subs.shift();
          str = str.replace(/\?/, label);
        }
      }
    }

    return {
      string: str,
      literals: literals,
      match: function(test) {
        return (test in literals) ? literals[test] : test;
      }
    };
  };

  // Shortcuts
  var _defaultStorage = 'local', _currentTable = null;

  // Parse a path argument, i.e 'local.foo'
  var _getPath = function(path) {
    var parts = path.split('.')
      , storage = _defaultStorage
      , table = parts[0];
    if (parts.length > 1) {
      storage = parts[0];
      table = parts[1];
    }
    return { storage: storage, table: table };
  };

  // Map path argument to a table object, also for creating new table objects
  var _getTable = function(path, createIfNotExist) {
    path = _getPath(path);
    _currentTable = _datastores[path.storage].tables[path.table];
    if (!_currentTable && createIfNotExist) {
      _currentTable = _datastores[path.storage].tables[path.table] = {rows: _createDataset(), fields: {}, auto_inc: 0};
    }
    else if (_currentTable) {
      _createDataset(_currentTable.rows);
    }
    return _currentTable;
  };

  // Serialize and store data
  var _commit = function() {
    Object.keys(_datastores).forEach(function(name) {
      //persist(name, _datastores[name])
    });
  };

  // Create hash of reserved keywords and compiled RegEx patterns
  var _keywords = (function() {
    var res = {}
      , keywords = 'SELECT,ORDER BY,DESC,ASC,INSERT INTO,UPDATE,SET,WHERE,AND,OR,DELETE FROM,LIMIT,VALUES'.split(',');
    forEach(keywords, function(kw) {
      res[kw] = new RegExp('(^|\\b)' + kw + '($|\\b)', 'gi');
    });
    return res;
  })();

  // Normalize passed in argument
  var _parseQuery = function(dql, subs) {
    var extract = extractLiterals(dql, subs);
    dql = extract.string.
        // Remove space around modifiers
        replace(/\s*([^a-z0-9_()*]+)\s*/gi, '$1').
        // Add delimiters around operators
        replace(/([!=<>]+)/gi, '#$1#').
        // Remove braces
        replace(/(\()\s*|\s*(\))/g, '$1$2').
        // Remove double spaces
        replace(/\s+/g, ' ').
        // Trim
        replace(/^\s+|\s+$/, '');
    // Uppercase keywords and convert spaces to underscores
    forEach(_keywords, function(keyword, patt) {
      var m = patt.exec(dql);
      patt.lastIndex = 0;
      if (m) {
        dql = dql.substring(0, m.index) +
          m[0].toUpperCase().replace(/\s/g, '_') +
          dql.substring(m.index + m[0].length);
      }
    });
    return {
      extract: extract,
      tokens: dql.split(' ')
    };
  };

  var _comp = {
    '='  : function(a, b) { return a == b; },
    '>'  : function(a, b) { return a > b; },
    '>=' : function(a, b) { return a >= b; },
    '<'  : function(a, b) { return a < b; },
    '<=' : function(a, b) { return a <= b; },
    '!=' : function(a, b) { return a != b; }
  };

  // Evaluate WHERE/AND/OR clauses, handle nested expressions
  var _evalWhere = function(clause, row, feed) {
    var evaluate = function(str) {
      var tokens = str.split(' ');
      for(var i = 0; i < tokens.length; i++) {
        var logicalNext = tokens[i+1]
          , result = tokens[i];
        if (/^[01]$/.test(result)) {
          result = +result;
        }
        else {
          var	parts = tokens[i].split('#');
          // Restore literals
          parts[2] = feed.extract.match(parts[2]); // ['id', '<', '123']
          // Do comparison
          result = _comp[parts[1]](row[parts[0]], parts[2]);
        }
        // Success
        if (result && (!logicalNext || logicalNext === 'OR')) {
          return true;
        }
        // Fail
        if (!result && (!logicalNext || logicalNext === 'AND')) {
          return false;
        }
        if (logicalNext) {
          i++;
        }
      }
    };
    // Deal with braced expressions
    if (clause.indexOf('(') !== -1) {
      var parensPatt = /\(([^\)]+)\)/g, m;
      while (clause.indexOf('(') !== -1) {
        parensPatt.lastIndex = clause.lastIndexOf('(');
        m = parensPatt.exec(clause);
        clause = clause.substring(0, m.index) +
          // Cast result to 1 or 0
          (+evaluate(m[1])) +
          clause.substring(m.index + m[0].length);
      }
    }
    return evaluate(clause);
  };

  // If a table schema is defined, make rows comply to it
  var _validateRow = function(row) {
    var fields = _currentTable.fields;
    // If no fields are defined in the schema, just return the row
    if (!Object.keys(fields).length) {
      return row;
    }
    // Schema defined fields
    forEach(fields, function(field, meta) {
      // Schema fields with attributes
      if (Object.keys(meta).length) {
        forEach(meta, function(attr, value) {
          if (value) {
            row[field] = (function() {
              switch (attr) {
                case 'auto_inc': return ++_currentTable.auto_inc;
                case 'timestamp': return +(new Date);
                case 'def':
                  if (!(field in row)) {
                    return value;
                  }
              }
            })();
          }
        });
      }
      // If a schema field has no attributes and has not been given a value
      else if (!(field in row)) {
        row[field] = null;
      }
    });
    // Delete rows that are not defined in the schema
    forEach(row, function(name) {
      if (!(name in fields)) {
        delete row[name];
      }
    });
    return row;
  };

  // Sugar for handling result sets
  var _sugarMethods = {
    each: function(func) {
      return forEach(this, func);
    },
    toString: function() {
      var out = [];
      forEach(this, function(row, i) {
        out.push('[' + i + ']');
        forEach(row, function(field, value) {
          out.push('\t' + field + ':');
          out.push('\t  ' + value);
        });
      });
      return out.join('\n');
    },
    log: function() {
      if (console) {
        // IE doesn't override toString method
        console.log(_sugarMethods.toString.call(this));
      }
    }
  };

  // Binds sugar methods to datasets and optionally creates them
  var _createDataset = function(rows) {
    var dataset = rows || [];
    forEach(_sugarMethods, function(name, method) {
      dataset[name] = method;
    });
    return dataset;
  };

  var _commandParsers = {

    'SELECT': function(feed) {
      var tokens = feed.tokens
        , rows = _currentTable.rows
        , fields = feed.args === '*' ? '*' : feed.args.split(',')
        , result = []
        , i = 0;

      // WHERE
      forEach(_currentTable.rows, function(row) {
        if (!feed.where || _evalWhere(feed.where, row, feed)) {
          result.push(row);
        }
      });
      // ORDER BY
      if (tokens[0] === 'ORDER_BY') {
        tokens.shift();
        var args = tokens.shift().split(',')
          , index = 0
          , sortKind = 'ASC'
          , sortComp = {
            'ASC': function(a, b) { return a[args[index]] > b[args[index]]; },
            'DESC': function(a, b) { return a[args[index]] < b[args[index]]; }
          }
          , sorter = function(a, b) {
            if (a[args[index]] === b[args[index]]) {
              if (args[index+1]) {
                index++;
                return sorter(a, b);
              }
              index = 0;
              return 0;
            }
            var result = sortComp[sortKind](a, b) ? 1 : -1;
            index = 0;
            return result;
          };
        if (tokens[0] in sortComp) {
          sortKind = tokens.shift();
        }
        result.sort(sorter);
      }
      // LIMIT
      if (tokens[0] === 'LIMIT') {
        tokens.shift();
        result = result.slice(0, tokens.shift());
      }
      // Truncate returned fields
      if (fields !== '*') {
        forEach(result, function(row) {
          for (var field in row) {
            if (fields.indexOf(field) < 0) {
              delete row[field];
            }
          }
        });
      }
      return _createDataset(result);
    },

    'DELETE_FROM': function(feed) {
      var newSet = [];
      forEach(_currentTable.rows, function(row, i) {
        if (!feed.where || _evalWhere(feed.where, row, feed)) {
          // console.log('skip');
        }
        else {
          newSet.push(row);
        }
      });
      _currentTable.rows = newSet;
      _commit();
      return  _currentTable.rows;
    },

    'UPDATE': function(feed) {
      feed.tokens.shift();
      var dataset = _currentTable.rows;
      var updates = (function() {
          var result = {};
          forEach(feed.tokens.shift().split(','), function(part) {
            var parts = part.split('#');
            result[parts[0]] = feed.extract.match(parts[2]);
          });
          return result;
        })();
      forEach(dataset, function(row) {
        if (!feed.where || _evalWhere(feed.where, row, feed)) {
          forEach(updates, function(name, value) {
            row[name] = value;
          });
        }
      });
      _commit();
      return dataset;
    },

    'INSERT_INTO': function(feed) {
      var fields = feed.tokens.shift().replace(/[()]/g, '').split(',')
        , values = feed.tokens.pop().replace(/[()]/g, '').split(',')
        , dataset = _currentTable.rows
        , row = {};
      // Restore any literal values
      forEach(fields, function(field, i) {
        row[field] = feed.extract.match(values[i]);
      });
      dataset.push(_validateRow(row));
      _commit();
      return dataset;
    }
  };


  // Public methods
  exports.SQL = {

    // Define a table schema, if table is already defined does nothing
    defineTable: function(path, fields) {
      if (this.tableExists(path)) {
        return;
      }
      // Create empty table
      _getTable(path, true);
      // Loop table schema
      forEach(fields || [], function(field) {
        var extract = extractLiterals(field)
          , parts = extract.string.replace(/\s+/g, ' ').split(' ')
          , token;
        field = _currentTable.fields[parts.shift()] = {};
        while (token = parts.shift()) {
          switch (token.toLowerCase()) {
            case 'auto_inc':
              field.auto_inc = true;
              break;
            case 'timestamp':
              field.timestamp = true;
              break;
            case 'default':
              field.def = extract.match(parts.shift());
          }
        }
      });
      _commit();
    },

    tableExists: function(tableName) {
      return !!_getTable(tableName);
    },

    showTables: function() {
      var out = [];
      forEach(_datastores, function(name) {
        out.push('[' + name + ']');
        forEach(_datastores[name].tables, function(table) {
          out.push('\t' + table);
        });
      });
      return out.join('\n');
    },

    dropTable: function(path) {
      path = _getPath(path);
      delete _datastores[path.storage].tables[path.table];
      _commit();
    },

    // Convenient alternative for stuffing data into tables
    insert: function(tableName, args) {
      var row;
      _getTable(tableName, true);
      while(row = args.shift()) {
        _currentTable.rows.push(_validateRow(row));
      }
      _commit();
      return _currentTable.rows;
    },

    query: function(dql, subs) {
      var feed = _parseQuery(dql, subs)
        , tokens = feed.tokens
        , command = tokens.shift();
      if (command === 'SELECT') {
        feed.args = tokens.shift();
        tokens.shift();
      }
      _getTable(tokens.shift(), true);
      // Extract WHERE/AND/OR clauses
      var where = [],	i = 0, token;
      for (i; i < tokens.length; i++) {
        if (tokens[i] === 'WHERE') {
          tokens.splice(i, 1);
          // 'id<123' 'AND' 'some=12'
          // 'id<123' 'AND' 'some=12' 'order_by' 'date' 'asc' 'limit' '10'
          while (token = tokens.splice(i, 1)[0]) {
            where.push(token);
            if (tokens[0] && /^(ORDER_BY|LIMIT)$/.test(tokens[0])) {
              break;
            }
          }
          break;
        }
      }
      feed.where = where.join(' ');
      return _commandParsers[command](feed);
    }
  };

})(window);