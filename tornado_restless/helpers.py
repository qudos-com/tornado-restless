import datetime
import inspect

from dateutil.parser import parse as parse_datetime
from sqlalchemy import Date, DateTime, Interval
from sqlalchemy.ext.associationproxy import AssociationProxy
from sqlalchemy.orm import RelationshipProperty as RelProperty
from sqlalchemy.orm import ColumnProperty
from sqlalchemy.orm.attributes import QueryableAttribute
from sqlalchemy.sql import func
from sqlalchemy.sql.expression import ColumnElement

RELATION_BLACKLIST = ('query', 'query_class', '_sa_class_manager',
                      '_decl_class_registry')

CURRENT_TIME_MARKERS = ('CURRENT_TIMESTAMP', 'CURRENT_DATE', 'LOCALTIMESTAMP')


def get_field_type(model, fieldname):
    """Helper which returns the SQLAlchemy type of the field.
    """
    field = getattr(model, fieldname)
    if isinstance(field, ColumnElement):
        fieldtype = field.type
    else:
        if isinstance(field, AssociationProxy):
            field = field.remote_attr
        if hasattr(field, 'property'):
            prop = field.property
            if isinstance(prop, RelProperty):
                return None
            fieldtype = prop.columns[0].type
        else:
            return None
    return fieldtype


def is_date_field(model, fieldname):
    """Returns ``True`` if and only if the field of `model` with the specified
    name corresponds to either a :class:`datetime.date` object or a
    :class:`datetime.datetime` object.
    """
    fieldtype = get_field_type(model, fieldname)
    return isinstance(fieldtype, Date) or isinstance(fieldtype, DateTime)


def is_interval_field(model, fieldname):
    """Returns ``True`` if and only if the field of `model` with the specified
    name corresponds to a :class:`datetime.timedelta` object.
    """
    fieldtype = get_field_type(model, fieldname)
    return isinstance(fieldtype, Interval)


def get_relations(model):
    """Returns a list of relation names of `model` (as a list of strings)."""
    return [k for k in dir(model)
            if not (k.startswith('__') or k in RELATION_BLACKLIST)
            and get_related_model(model, k)]


def get_related_model(model, relationname):
    """Gets the class of the model to which `model` is related by the attribute
    whose name is `relationname`.
    """
    if hasattr(model, relationname):
        attr = getattr(model, relationname)
        if hasattr(attr, 'property') \
                and isinstance(attr.property, RelProperty):
            return attr.property.mapper.class_
        if isinstance(attr, AssociationProxy):
            return get_related_association_proxy_model(attr)
    return None


def get_related_association_proxy_model(attr):
    """Returns the model class specified by the given SQLAlchemy relation
    attribute, or ``None`` if no such class can be inferred.
    `attr` must be a relation attribute corresponding to an association proxy.
    """
    prop = attr.remote_attr.property
    for attribute in ('mapper', 'parent'):
        if hasattr(prop, attribute):
            return getattr(prop, attribute).class_
    return None


def get_or_create(session, model, attrs):
    """Returns the single instance of `model` whose primary key has the
    value found in `attrs`, or initializes a new instance if no primary key
    is specified.
    Before returning the new or existing instance, its attributes are
    assigned to the values supplied in the `attrs` dictionary.
    This method does not commit the changes made to the session; the
    calling function has that responsibility.
    """
    # Not a full relation, probably just an association proxy to a scalar
    # attribute on the remote model.
    if not isinstance(attrs, dict):
        return attrs
    # Recurse into nested relationships
    for rel in get_relations(model):
        if rel not in attrs:
            continue
        if isinstance(attrs[rel], list):
            attrs[rel] = [get_or_create(session, get_related_model(model, rel),
                                        r) for r in attrs[rel]]
        else:
            attrs[rel] = get_or_create(session, get_related_model(model, rel),
                                       attrs[rel])
    # Find private key names
    pk_names = primary_key_names(model)
    attrs = strings_to_dates(model, attrs)
    # If all of the primary keys were included in `attrs`, try to update
    # an existing row.
    if all(k in attrs for k in pk_names):
        # Determine the sub-dictionary of `attrs` which contains the mappings
        # for the primary keys.
        pk_values = dict((k, v) for (k, v) in attrs.items()
                         if k in pk_names)
        # query for an existing row which matches all the specified
        # primary key values.
        instance = session_query(session, model).filter_by(**pk_values).first()
        if instance is not None:
            assign_attributes(instance,
                              **{k: v for k, v in attrs.iteritems()
                                 if k not in pk_names})
            return instance
    # If some of the primary keys were missing, or the row wasn't found,
    # create a new row.
    return model(**attrs)


def session_query(session, model):
    """Returns a SQLAlchemy query object for the specified `model`.
    If `model` has a ``query`` attribute already, ``model.query`` will be
    returned. If the ``query`` attribute is callable ``model.query()`` will be
    returned instead.
    If `model` has no such attribute, a query based on `session` will be
    created and returned.
    """
    if hasattr(model, 'query'):
        if callable(model.query):
            query = model.query()
        else:
            query = model.query
        if hasattr(query, 'filter'):
            return query
    return session.query(model)


def assign_attributes(model, **kwargs):
    """Assign all attributes from the supplied `kwargs` dictionary to the
    model. This does the same thing as the default declarative constructor,
    when provided a dictionary of attributes and values.
    """
    cls = type(model)
    for field, value in kwargs.items():
        if not hasattr(cls, field):
            msg = '{0} has no field named "{1!r}"'.format(cls.__name__, field)
            raise TypeError(msg)
        setattr(model, field, value)


def primary_key_names(model):
    """Returns all the primary keys for a model."""
    return [key for key, field in inspect.getmembers(model)
            if isinstance(field, QueryableAttribute)
            and isinstance(field.property, ColumnProperty)
            and field.property.columns[0].primary_key]


def strings_to_dates(model, dictionary):
    """Returns a new dictionary with all the mappings of `dictionary` but
    with date strings and intervals mapped to :class:`datetime.datetime` or
    :class:`datetime.timedelta` objects.
    The keys of `dictionary` are names of fields in the model specified in the
    constructor of this class. The values are values to set on these fields. If
    a field name corresponds to a field in the model which is a
    :class:`sqlalchemy.types.Date`, :class:`sqlalchemy.types.DateTime`, or
    :class:`sqlalchemy.Interval`, then the returned dictionary will have the
    corresponding :class:`datetime.datetime` or :class:`datetime.timedelta`
    Python object as the value of that mapping in place of the string.
    This function outputs a new dictionary; it does not modify the argument.
    """
    result = {}
    for fieldname, value in dictionary.items():
        if is_date_field(model, fieldname) and value is not None:
            if value.strip() == '':
                result[fieldname] = None
            elif value in CURRENT_TIME_MARKERS:
                result[fieldname] = getattr(func, value.lower())()
            else:
                value_as_datetime = parse_datetime(value)
                result[fieldname] = value_as_datetime
                # If the attribute on the model needs to be a Date object as
                # opposed to a DateTime object, just get the date component of
                # the datetime.
                fieldtype = get_field_type(model, fieldname)
                if isinstance(fieldtype, Date):
                    result[fieldname] = value_as_datetime.date()
        elif (is_interval_field(model, fieldname) and value is not None
              and isinstance(value, int)):
            result[fieldname] = datetime.timedelta(seconds=value)
        else:
            result[fieldname] = value
    return result
