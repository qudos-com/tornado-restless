#!/usr/bin/python
# -*- encoding: utf-8 -*-
"""

"""
import collections
from datetime import datetime, date, time
import inspect
import itertools
from uuid import UUID

from .errors import IllegalArgumentError, DictConvertionError
from .helpers import get_related_association_proxy_model
from .helpers import primary_key_names
from .helpers import session_query
from .wrapper import ModelWrapper
from geoalchemy2.elements import WKTElement, WKBElement
from geoalchemy2.shape import to_shape
from geojson.mapping import to_mapping
from sqlalchemy.orm import object_mapper
from sqlalchemy.orm.attributes import InstrumentedAttribute
from sqlalchemy.orm.exc import UnmappedInstanceError
from sqlalchemy.orm.query import Query


__author__ = 'Martin Martimeo <martin@martimeo.de>'
__date__ = '23.05.13 - 17:41'

__datetypes__ = (datetime, time, date)
__basetypes__ = (basestring, int, bool, float, long)


class Filter(object):
    """Represents a filter to apply to a SQL query.
    A filter can be, for example, a comparison operator applied to a field of a
    model and a value or a comparison applied to two fields of the same
    model. For more information on possible filters, see :ref:`search`.
    """

    def __init__(self, fieldname, operator, argument=None, otherfield=None):
        """Instantiates this object with the specified attributes.
        `fieldname` is the name of the field of a model which will be on the
        left side of the operator.
        `operator` is the string representation of an operator to apply. The
        full list of recognized operators can be found at :ref:`search`.
        If `argument` is specified, it is the value to place on the right side
        of the operator. If `otherfield` is specified, that field on the model
        will be placed on the right side of the operator.
        .. admonition:: About `argument` and `otherfield`
           Some operators don't need either argument and some need exactly one.
           However, this constructor will not raise any errors or otherwise
           inform you of which situation you are in; it is basically just a
           named tuple. Calling code must handle errors caused by missing
           required arguments.
        """
        self.fieldname = fieldname
        self.operator = operator
        self.argument = argument
        self.otherfield = otherfield

    def __repr__(self):
        """Returns a string representation of this object."""
        return '<Filter {0} {1} {2}>'.format(self.fieldname, self.operator,
                                             self.argument or self.otherfield)

    @staticmethod
    def from_dictionary(dictionary):
        """Returns a new :class:`Filter` object with arguments parsed from
        `dictionary`.
        `dictionary` is a dictionary of the form::
            {'name': 'age', 'op': 'lt', 'val': 20}
        or::
            {'name': 'age', 'op': 'lt', 'other': 'height'}
        where ``dictionary['name']`` is the name of the field of the model on
        which to apply the operator, ``dictionary['op']`` is the name of the
        operator to apply, ``dictionary['val']`` is the value on the right to
        which the operator will be applied, and ``dictionary['other']`` is the
        name of the other field of the model to which the operator will be
        applied.
        'dictionary' may also be an arbitrary Boolean formula consisting of
        dictionaries such as these. For example::
            {'or':
                 [{'and':
                       [dict(name='name', op='like', val='%y%'),
                        dict(name='age', op='ge', val=10)]},
                  dict(name='name', op='eq', val='John')
                  ]
             }
        """
        # If there are no ANDs or ORs, we are in the base case of the
        # recursion.
        if 'or' not in dictionary and 'and' not in dictionary:
            fieldname = dictionary.get('name')
            operator = dictionary.get('op')
            argument = dictionary.get('val')
            otherfield = dictionary.get('field')
            return Filter(fieldname, operator, argument, otherfield)
        # For the sake of brevity, rename this method.
        from_dict = Filter.from_dictionary
        # If there is an OR or an AND in the dictionary, recurse on the
        # provided list of filters.
        if 'or' in dictionary:
            subfilters = dictionary.get('or')
            return DisjunctionFilter(*(from_dict(f) for f in subfilters))
        if 'and' in dictionary:
            subfilters = dictionary.get('and')
            return ConjunctionFilter(*(from_dict(f) for f in subfilters))


class JunctionFilter(Filter):
    def __init__(self, *subfilters):
        self.subfilters = subfilters
    def __iter__(self):
        return iter(self.subfilters)


class ConjunctionFilter(JunctionFilter):
    def __repr__(self):
        return 'and_{0}'.format(tuple(repr(f) for f in self))


class DisjunctionFilter(JunctionFilter):
    def __repr__(self):
        return 'or_{0}'.format(tuple(repr(f) for f in self))


def _sub_operator(model, argument, fieldname):
    """Recursively calls :func:`QueryBuilder._create_operation` when argument
    is a dictionary of the form specified in :ref:`search`.
    This function is for use with the ``has`` and ``any`` search operations.
    """
    if isinstance(model, InstrumentedAttribute):
        submodel = model.property.mapper.class_
    elif isinstance(model, AssociationProxy):
        submodel = get_related_association_proxy_model(model)
    else:  # TODO what to do here?
        pass
    if isinstance(argument, dict):
        fieldname = argument['name']
        operator = argument['op']
        argument = argument.get('val')
        relation = None
        if '__' in fieldname:
            fieldname, relation = fieldname.split('__')
        return QueryBuilder._create_operation(submodel, fieldname, operator,
                                              argument, relation)
    # Support legacy has/any with implicit eq operator
    return getattr(submodel, fieldname) == argument


OPERATORS = {
    # Operators which accept a single argument.
    'is_null': lambda f: f == None,
    'is_not_null': lambda f: f != None,
    # TODO what are these?
    'desc': lambda f: f.desc,
    'asc': lambda f: f.asc,
    # Operators which accept two arguments.
    '==': lambda f, a: f == a,
    'eq': lambda f, a: f == a,
    'equals': lambda f, a: f == a,
    'equal_to': lambda f, a: f == a,
    '!=': lambda f, a: f != a,
    'ne': lambda f, a: f != a,
    'neq': lambda f, a: f != a,
    'not_equal_to': lambda f, a: f != a,
    'does_not_equal': lambda f, a: f != a,
    '>': lambda f, a: f > a,
    'gt': lambda f, a: f > a,
    '<': lambda f, a: f < a,
    'lt': lambda f, a: f < a,
    '>=': lambda f, a: f >= a,
    'ge': lambda f, a: f >= a,
    'gte': lambda f, a: f >= a,
    'geq': lambda f, a: f >= a,
    '<=': lambda f, a: f <= a,
    'le': lambda f, a: f <= a,
    'lte': lambda f, a: f <= a,
    'leq': lambda f, a: f <= a,
    'ilike': lambda f, a: f.ilike(a),
    'like': lambda f, a: f.like(a),
    'in': lambda f, a: f.in_(a),
    'not_in': lambda f, a: ~f.in_(a),
    # Operators which accept three arguments.
    'has': lambda f, a, fn: f.has(_sub_operator(f, a, fn)),
    'any': lambda f, a, fn: f.any(_sub_operator(f, a, fn)),
}


class QueryBuilder(object):
    """Provides a static function for building a SQLAlchemy query object based
    on a :class:`SearchParameters` instance.
    Use the static :meth:`create_query` method to create a SQLAlchemy query on
    a given model.
    """

    @staticmethod
    def _create_operation(model, fieldname, operator, argument, relation=None):
        """Translates an operation described as a string to a valid SQLAlchemy
        query parameter using a field or relation of the specified model.
        More specifically, this translates the string representation of an
        operation, for example ``'gt'``, to an expression corresponding to a
        SQLAlchemy expression, ``field > argument``. The recognized operators
        are given by the keys of :data:`OPERATORS`. For more information on
        recognized search operators, see :ref:`search`.
        If `relation` is not ``None``, the returned search parameter will
        correspond to a search on the field named `fieldname` on the entity
        related to `model` whose name, as a string, is `relation`.
        `model` is an instance of a SQLAlchemy declarative model being
        searched.
        `fieldname` is the name of the field of `model` to which the operation
        will be applied as part of the search. If `relation` is specified, the
        operation will be applied to the field with name `fieldname` on the
        entity related to `model` whose name, as a string, is `relation`.
        `operation` is a string representating the operation which will be
         executed between the field and the argument received. For example,
         ``'gt'``, ``'lt'``, ``'like'``, ``'in'`` etc.
        `argument` is the argument to which to apply the `operator`.
        `relation` is the name of the relationship attribute of `model` to
        which the operation will be applied as part of the search, or ``None``
        if this function should not use a related entity in the search.
        This function raises the following errors:
        * :exc:`KeyError` if the `operator` is unknown (that is, not in
          :data:`OPERATORS`)
        * :exc:`TypeError` if an incorrect number of arguments are provided for
          the operation (for example, if `operation` is `'=='` but no
          `argument` is provided)
        * :exc:`AttributeError` if no column with name `fieldname` or
          `relation` exists on `model`
        """

        # raises KeyError if operator not in OPERATORS
        opfunc = OPERATORS[operator]
        # In Python 3.0 or later, this should be `inspect.getfullargspec`
        # because `inspect.getargspec` is deprecated.
        numargs = len(inspect.getargspec(opfunc).args)
        # raises AttributeError if `fieldname` or `relation` does not exist
        field = getattr(model, relation or fieldname)
        # each of these will raise a TypeError if the wrong number of argments
        # is supplied to `opfunc`.
        if numargs == 1:
            return opfunc(field)
        if argument is None:
            msg = ('To compare a value to NULL, use the is_null/is_not_null '
                   'operators.')
            raise TypeError(msg)
        if numargs == 2:
            return opfunc(field, argument)
        return opfunc(field, argument, fieldname)

    @staticmethod
    def _create_filter(model, filt):
        """Returns the operation on `model` specified by the provided filter.
        `filt` is an instance of the :class:`Filter` class.
        Raises one of :exc:`AttributeError`, :exc:`KeyError`, or
        :exc:`TypeError` if there is a problem creating the query. See the
        documentation for :func:`_create_operation` for more information.
        """
        # If the filter is not a conjunction or a disjunction, simply proceed
        # as normal.
        if not isinstance(filt, JunctionFilter):
            fname = filt['name']
            val = filt['value']
            # get the relationship from the field name, if it exists
            relation = None
            if '__' in fname:
                relation, fname = fname.split('__')
            # # get the other field to which to compare, if it exists
            if filt.get('otherfield'):
                val = getattr(model, filt['otherfield'])
            # for the sake of brevity...
            create_op = QueryBuilder._create_operation
            return create_op(model, fname, filt['op'], val, relation)
        # Otherwise, if this filter is a conjunction or a disjunction, make
        # sure to apply the appropriate filter operation.
        create_filt = QueryBuilder._create_filter
        if isinstance(filt, ConjunctionFilter):
            return and_(create_filt(model, f) for f in filt)
        return or_(create_filt(model, f) for f in filt)

    @staticmethod
    def create_query(session, model, search_params, _ignore_order_by=False):
        """Builds an SQLAlchemy query instance based on the search parameters
        present in ``search_params``, an instance of :class:`SearchParameters`.
        This method returns a SQLAlchemy query in which all matched instances
        meet the requirements specified in ``search_params``.
        `model` is SQLAlchemy declarative model on which to create a query.
        `search_params` is an instance of :class:`SearchParameters` which
        specify the filters, order, limit, offset, etc. of the query.
        If `_ignore_order_by` is ``True``, no ``order_by`` method will be
        called on the query, regardless of whether the search parameters
        indicate that there should be an ``order_by``. (This is used internally
        by Flask-Restless to work around a limitation in SQLAlchemy.)
        Building the query proceeds in this order:
        1. filtering
        2. ordering
        3. grouping
        3. limiting
        4. offsetting
        Raises one of :exc:`AttributeError`, :exc:`KeyError`, or
        :exc:`TypeError` if there is a problem creating the query. See the
        documentation for :func:`_create_operation` for more information.
        """
        query = session_query(session, model)
        # For the sake of brevity, rename this method.
        create_filt = QueryBuilder._create_filter
        # This function call may raise an exception.
        filters = [create_filt(model, filt) for filt in search_params.filters]
        # Multiple filter criteria at the top level of the provided search
        # parameters are interpreted as a conjunction (AND).
        query = query.filter(*filters)

        # Order the search. If no order field is specified in the search
        # parameters, order by primary key.
        if not _ignore_order_by:
            if search_params.order_by:
                for val in search_params.order_by:
                    field_name = val.field
                    if '__' in field_name:
                        field_name, field_name_in_relation = \
                            field_name.split('__')
                        relation = getattr(model, field_name)
                        relation_model = relation.mapper.class_
                        field = getattr(relation_model, field_name_in_relation)
                        direction = getattr(field, val.direction)
                        query = query.join(relation_model)
                        query = query.order_by(direction())
                    else:
                        field = getattr(model, val.field)
                        direction = getattr(field, val.direction)
                        query = query.order_by(direction())
            else:
                pks = primary_key_names(model)
                pk_order = (getattr(model, field).asc() for field in pks)
                query = query.order_by(*pk_order)

        # Group the query.
        if search_params.group_by:
            for groupby in search_params.group_by:
                field = getattr(model, groupby.field)
                query = query.group_by(field)

        # Apply limit and offset to the query.
        if search_params.limit:
            query = query.limit(search_params.limit)
        if search_params.offset:
            query = query.offset(search_params.offset)

        return query


def build_query(session,
                model,
                filters=None,
                order_by=None):
    """
        Returns a query using passed filters and order_bys

        :param session:
        :param model:
        :param filters: List of filters in restless 3-tuple op string format
        :param order_by: List of orders to be appended aswell
    """

    query = session_query(session, model)

    # build filters
    if filters:
        create_filt = QueryBuilder._create_filter
        filters = [create_filt(model, filt) for filt in filters]
        query = query.filter(*filters)

    # Order the search. If no order field is specified in the search
    # parameters, order by primary key.
    if order_by:
        for val in order_by:
            field_name = val['field']
            if '__' in field_name:
                field_name, field_name_in_relation = \
                    field_name.split('__')
                relation = getattr(model, field_name)
                relation_model = relation.mapper.class_
                field = getattr(relation_model, field_name_in_relation)
                direction = getattr(field, val['direction'])
                query = query.join(relation_model)
                query = query.order_by(direction())
            else:
                field = getattr(model, val['field'])
                direction = getattr(field, val['direction'])
                query = query.order_by(direction())
    else:
        pks = primary_key_names(model)
        pk_order = (getattr(model, field).asc() for field in pks)
        query = query.order_by(*pk_order)

    return query


def parse_columns(strings):
    """
        Parse a list of column names (name1, name2, relation.name1, ...)

        :param strings: List of Column Names
        :return:
    """
    columns = {}

    # Strings
    if strings is None:
        return None

    # Parse
    for column in [column.split(".", 1) for column in strings]:
        if len(column) == 1:
            columns[column[0]] = True
        else:
            columns.setdefault(column[0], []).append(column[1])

    # Now parse relations
    for (key, item) in columns.items():
        if isinstance(item, list):
            columns[key] = parse_columns(item)

    # Return
    return columns


def combine_columns(requested, include):
    """
        Combine two sets of column definitions created by parse_columns
    """
    combined = {}
    for k, v in requested.iteritems():
        include_value = include.get(k)
        if not include_value:
            continue
        elif include_value is True:
            combined[k] = v
        elif v is True:
            combined[k] = include_value
        else:
            combined[k] = combine_columns(v, include_value)
    return combined


def to_deep(include,
            exclude,
            key):
    """
        Extract the include/exclude information for key

        :param include: Columns and Relations that should be included for an instance
        :param exclude: Columns and Relations that should not be included for an instance
        :param key: The key that should be extracted
    """
    rtn = {}

    try:
        rtn['include'] = include.setdefault(key, False)
    except AttributeError:
        rtn['include'] = False

    try:
        rtn['exclude'] = exclude[key]
        rtn['include'] = None
    except (TypeError, KeyError):
        rtn['exclude'] = None

    return rtn


def to_dict(instance,
            options=collections.defaultdict(bool),
            include=None,
            exclude=None):
    """
        Translates sqlalchemy instance to dictionary

        Inspired by flask-restless.helpers.to_dict

        :param instance:
        :param options: Dictionary of flags
                          * execute_queries: Execute Query Objects
                          * execute_hybrids: Execute Hybrids
        :param include: Columns and Relations that should be included for an instance
        :param exclude: Columns and Relations that should not be included for an instance
    """
    if exclude is not None and include is not None:
        raise ValueError('Cannot specify both include and exclude.')

    # None
    if instance is None:
        return None

    # Int / Float / Str
    if isinstance(instance, __basetypes__):
        return instance

    # Date & Time
    if isinstance(instance, __datetypes__):
        return instance.isoformat()

    # A Geoalchemy element
    if isinstance(instance, (WKTElement, WKBElement)):
        return to_mapping(to_shape(instance))

    # A UUID
    if isinstance(instance, UUID):
        return unicode(instance)

    # Any Dictionary
    if isinstance(instance, dict) or hasattr(instance, 'items'):
        return {k: to_dict(v, options=options, **to_deep(include, exclude, k)) for k, v in instance.items()}

    # Any List
    if isinstance(instance, list) or hasattr(instance, '__iter__'):
        return [to_dict(x, options=options, include=include, exclude=exclude) for x in instance]

    # Include Columns given
    if isinstance(include, collections.Iterable):
        rtn = {}
        for column in include:
            rtn[column] = to_dict(getattr(instance, column), **to_deep(include, exclude, column))
        return rtn

    # Include all columns if it is a SQLAlchemy instance
    try:
        columns = ModelWrapper.get_columns(object_mapper(instance)).keys()
        relations = ModelWrapper.get_relations(object_mapper(instance)).keys()
        attributes = ModelWrapper.get_attributes(object_mapper(instance)).keys()
        proxies = [p.key for p in ModelWrapper.get_proxies(object_mapper(instance))]
        hybrids = [p.key for p in ModelWrapper.get_hybrids(object_mapper(instance))]
        attributes = itertools.chain(columns, relations, proxies, hybrids, attributes)
    except UnmappedInstanceError:
        raise DictConvertionError("Could not convert argument to plain dict")

    rtn = {}

    # Include AssociationProxy and Hybrids (may be list/dict/col)
    for column in attributes:

        if exclude is not None and column in exclude and exclude[column] is True:
            continue
        if column in rtn:
            continue

        # Prevent unnec. db calls
        if include is False and column not in hybrids and column not in columns:
            continue

        if column not in instance.__dict__ and not options.get('execute_queries', True):
            if column not in hybrids or not options.get('execute_hybrids', True):
                continue

        # Get Attribute
        node = getattr(instance, column)

        # Don't execute queries if stopping deepnes
        if include is False and isinstance(node, Query):
            continue
        # Otherwise query it
        elif isinstance(node, Query) and options['execute_queries']:
            node = node.all()

        # Convert it
        rtn[column] = to_dict(node, **to_deep(include, exclude, column))
    return rtn
