"""A Crossplane composition function."""

import asyncio
import base64
import builtins
import importlib
import inspect
import sys

import grpc
import crossplane.function.logging
import crossplane.function.response
from crossplane.function.proto.v1 import run_function_pb2 as fnv1
from crossplane.function.proto.v1 import run_function_pb2_grpc as grpcv1
from .. import pythonic

builtins.BaseComposite = pythonic.BaseComposite
builtins.Map = pythonic.Map
builtins.List = pythonic.List
builtins.Unknown = pythonic.Unknown
builtins.Yaml = pythonic.Yaml
builtins.Json = pythonic.Json
builtins.B64Encode = pythonic.B64Encode
builtins.B64Decode = pythonic.B64Decode


class FunctionRunner(grpcv1.FunctionRunnerService):
    """A FunctionRunner handles gRPC RunFunctionRequests."""

    def __init__(self, debug=False):
        """Create a new FunctionRunner."""
        self.debug = debug
        self.logger = crossplane.function.logging.get_logger()
        self.clazzes = {}

    def invalidate_module(self, module):
        if module in sys.modules:
            del sys.modules[module]
        for composite in [composite for composite in self.clazzes.keys()]:
            if '\n' not in composite:
                if composite.rsplit('.', 1)[0] == module:
                    del self.clazzes[composite]

    async def RunFunction(
        self, request: fnv1.RunFunctionRequest, _: grpc.aio.ServicerContext
    ) -> fnv1.RunFunctionResponse:
        composite = request.observed.composite.resource
        logger = self.logger.bind(
            apiVersion=composite['apiVersion'],
            kind=composite['kind'],
            name=composite['metadata']['name'],
        )
        if request.meta.tag:
            logger = logger.bind(tag=request.meta.tag[:7])
        input = request.input
        if 'step' in input:
            logger = logger.bind(step=input['step'])
        logger.debug('Running')

        response = crossplane.function.response.to(request)

        if composite['apiVersion'] == 'pythonic.fortra.com/v1alpha1' and composite['kind'] == 'Composite':
            if 'composite' not in composite['spec']:
                logger.error('Missing spec "composite"')
                crossplane.function.response.fatal(response, 'Missing spec "composite"')
                return response
            composite = composite['spec']['composite']
        else:
            if 'composite' not in input:
                logger.error('Missing input "composite"')
                crossplane.function.response.fatal(response, 'Missing input "composite"')
                return response
            composite = input['composite']

        clazz = self.clazzes.get(composite)
        if not clazz:
            if '\n' in composite:
                module = Module()
                try:
                    exec(composite, module.__dict__)
                except Exception as e:
                    logger.exception('Exec exception')
                    crossplane.function.response.fatal(response, f"Exec exception: {e}")
                    return response
                composite = ['<script>', 'Composite']
            else:
                composite = composite.rsplit('.', 1)
                if len(composite) == 1:
                    logger.error(f"Composite class name does not include module: {composite[0]}")
                    crossplane.function.response.fatal(response, f"Composite class name does not include module: {composite[0]}")
                    return response
                try:
                    module = importlib.import_module(composite[0])
                except Exception as e:
                    logger.error(str(e))
                    crossplane.function.response.fatal(response, f"Import module exception: {e}")
                    return response
            clazz = getattr(module, composite[1], None)
            if not clazz:
                logger.error(f"{composite[0]} did not define: {composite[1]}")
                crossplane.function.response.fatal(response, f"{composite[0]} did not define: {composite[1]}")
                return response
            composite = '.'.join(composite)
            if not inspect.isclass(clazz):
                logger.error(f"{composite} is not a class")
                crossplane.function.response.fatal(response, f"{composite} is not a class")
                return response
            if not issubclass(clazz, BaseComposite):
                logger.error(f"{composite} is not a subclass of BaseComposite")
                crossplane.function.response.fatal(response, f"{composite} is not a subclass of BaseComposite")
                return response
            self.clazzes[composite] = clazz

        try:
            composite = clazz(request, response, logger)
        except Exception as e:
            logger.exception('Instatiate exception')
            crossplane.function.response.fatal(response, f"Instatiate exception: {e}")
            return response

        try:
            result = composite.compose()
            if asyncio.iscoroutine(result):
                await result
        except Exception as e:
            logger.exception('Compose exception')
            crossplane.function.response.fatal(response, f"Compose exception: {e}")
            return response

        unknownResources = []
        warningResources = []
        fatalResources = []
        for name, resource in sorted(entry for entry in composite.resources):
            unknowns = resource.desired._getUnknowns
            if unknowns:
                unknownResources.append(name)
                warning = False
                fatal = False
                if resource.observed:
                    warningResources.append(name)
                    warning = True
                    if resource.unknownsFatal:
                        fatalResources.append(name)
                        fatal = True
                    elif resource.unknownsFatal is None and composite.unknownsFatal:
                        fatalResources.append(name)
                        fatal = True
                if self.debug:
                    for destination, source in sorted(unknowns.items()):
                        destination = self._trimFullName('response', 'desired', destination)
                        source = self._trimFullName('request', 'observed', source)
                        if fatal:
                            logger.error('Observed unknown', destination=destination, source=source)
                        elif warning:
                            logger.warning('Observed unknown', destination=destination, source=source)
                        else:
                            logger.debug('New unknown', destination=destination, source=source)
                if resource.observed:
                    resource.desired._patchUnknowns(resource.observed)
                else:
                    del composite.resources[name]
        if fatalResources:
            if not self.debug:
                logger.error('Observed resources with unknowns', resources=fatalResources)
            message = f"Observed resources with unknowns: {','.join(fatalResources)}"
            composite.conditions.NoUnknowns(False, 'FatalUnknowns', message)
            composite.results.fatal(message, 'FatalUnknowns')
            return response
        if warningResources:
            if not self.debug:
                logger.warning('Observed resources with unknowns', resources=fatalResources)
            message = f"Observed resources with unknowns: {','.join(warningResources)}"
            composite.conditions.NoUnknowns(False, 'ObservedUnknowns', message)
            composite.results.warning(message, 'ObservedUnknowns')
        elif unknownResources:
            if not self.debug:
                logger.info('New resources with unknowns', resources=unknownResources)
            message = f"New resources with unknowns: {','.join(unknownResources)}"
            composite.conditions.NoUnknowns(False, 'NewUnknowns', message)
            composite.results.info(message, 'NewUnknowns')
        else:
            composite.conditions.NoUnknowns(True, 'AllResolved', 'All resources are resolved')

        for name, resource in composite.resources:
            if resource.autoReady or (resource.autoReady is None and composite.autoReady):
                if resource.ready is None:
                    if resource.conditions.Ready.status:
                        resource.ready = True

        logger.debug('Returning')
        return response

    def _trimFullName(self, message, state, name):
        name = name.split('.')
        ix = 0
        for value in (message, state, 'resources', None, 'resource'):
            if value and ix < len(value) and name[ix] == value:
                del name[ix]
            else:
                ix += 1
        return '.'.join(name)


class Module:
    pass
