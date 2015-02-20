#!/usr/bin/python
# -*- encoding: utf-8 -*-
"""

"""
from tornado.web import Application, URLSpec

from .handler import BaseHandler
from .errors import IllegalArgumentError

__author__ = 'Martin Martimeo <martin@martimeo.de>'
__date__ = '26.04.13 - 22:25'


class ApiManager(object):
    """
        The tornado restless engine

        You normally only need one instance of this class to spawn your tornado routes
    """

    METHODS_READ = frozenset(['GET'])
    METHODS_MODIFY = frozenset(['POST', 'PUT', 'PATCH'])
    METHODS_DELETE = frozenset(['DELETE'])

    METHODS_UPDATE = METHODS_READ | METHODS_MODIFY
    METHODS_ALL = METHODS_READ | METHODS_MODIFY | METHODS_DELETE

    def __init__(self,
                 application,
                 session_maker=None):
        """
        Create an instance of the tornado restless engine

        :param session_maker: is a sqlalchemy.orm.Session class factory
        :param application: is the tornado.web.Application object
        """
        self.application = application

        self.session_maker = session_maker

    def create_api_blueprint(self,
                             model,
                             methods=METHODS_READ,
                             preprocessor=None,
                             postprocessor=None,
                             url_prefix='/api',
                             url_path=None,
                             collection_name=None,
                             allow_patch_many=False,
                             allow_method_override=False,
                             validation_exceptions=None,
                             exclude_queries=False,
                             exclude_hybrids=False,
                             include_columns=None,
                             include_columns_many=None,
                             deferred_columns=None,
                             exclude_columns=None,
                             results_per_page=10,
                             max_results_per_page=100,
                             blueprint_prefix='',
                             handler_class=BaseHandler,
                             query_options=None):
        """
        Create a tornado route for a sqlalchemy model

        :param model: The sqlalchemy model
        :param methods: Allowed methods for this model
        :param url_prefix: The url prefix of the application
        :param collection_name:
        :param allow_patch_many: Allow PATCH with multiple datasets
        :param allow_method_override: Support X-HTTP-Method-Override Header
        :param validation_exceptions:
        :param exclude_queries: Don't execude dynamic queries (like from associations or lazy relations)
        :param exclude_hybrids: When exclude_queries is True and exclude_hybrids is False, hybrids are still included.
        :param include_columns: Whitelist of columns to be included
        :param include_columns_many: Whitelist of columns to be included for get_many requests (defaults to include_columns)
        :param exclude_columns: Blacklist of columns to be excluded
        :param results_per_page: The default value of how many results are returned per request
        :param max_results_per_page: The hard upper limit of resutest per page
        :param blueprint_prefix: The Prefix that will be used to unique collection_name for named_handlers
        :param preprocessor: A dictionary of list of preprocessors that get called
        :param postprocessor: A dictionary of list of postprocessor that get called
        :param handler_class: The Handler Class that will be used in the route
        :type handler_class: tornado_restless.handler.BaseHandler or a subclass
        :param query_options: An array of options to be appied to SQLAlchemy queries
        :return: :class:`tornado.web.URLSpec`
        :raise: IllegalArgumentError
        """
        if exclude_columns is not None and include_columns is not None:
            raise IllegalArgumentError('Cannot simultaneously specify both include columns and exclude columns.')

        kwargs = {'model': model,
                  'manager': self,
                  'methods': methods,
                  'preprocessor': preprocessor or {},
                  'postprocessor': postprocessor or {},
                  'allow_patch_many': allow_patch_many,
                  'allow_method_override': allow_method_override,
                  'validation_exceptions': validation_exceptions,
                  'include_columns': include_columns,
                  'include_columns_many': include_columns_many,
                  'deferred_columns': deferred_columns,
                  'exclude_columns': exclude_columns,
                  'exclude_queries': exclude_queries,
                  'exclude_hybrids': exclude_hybrids,
                  'results_per_page': results_per_page,
                  'max_results_per_page': max_results_per_page,
                  'query_options': query_options}

        if collection_name is None:
            collection_name = model.__tablename__
        if url_path is None:
            url_path = "%s(?:/(.+))?/?" % collection_name
        blueprint = URLSpec(
            r"%s/%s$" % (url_prefix, url_path),
            handler_class,
            kwargs,
            '%s%s' % (blueprint_prefix, collection_name))
        return blueprint

    def create_api(self,
                   model,
                   virtualhost=r".*$", *args, **kwargs):
        """
        Creates and registers a route for the model in your tornado application

        The positional and keyword arguments are passed directly to the create_api_blueprint method

        :param model:
        :param virtualhost: bindhost for binding, .*$ in default
        """
        blueprint = self.create_api_blueprint(model, *args, **kwargs)

        for vhost, handlers in self.application.handlers:
            if vhost == virtualhost:
                handlers.append(blueprint)
                break
        else:
            self.application.add_handlers(virtualhost, [blueprint])

        self.application.named_handlers[blueprint.name] = blueprint
