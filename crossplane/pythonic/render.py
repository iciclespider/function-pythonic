
import pathlib
import sys
import yaml
from crossplane.function.proto.v1 import run_function_pb2 as fnv1

from . import (
    command,
    function,
    protobuf,
)


class Command(command.Command):
    name = 'render'
    help = 'Render a function-pythonic Composition'

    @classmethod
    def add_parser_arguments(cls, parser):
        parser.add_argument(
            'composite',
            type=pathlib.Path,
            help='A YAML file specifying the composite resource (XR) to render.',
        )
        parser.add_argument(
            'composition',
            type=pathlib.Path,
            nargs='?',
            help='The function-pythonic Composition to use to render the XR.',
        )
        parser.add_argument(
            '--context-files',
            action='append',
            default=[],
            metavar='KEY=PATH',
            help='Context key-value pairs to pass to the Function pipeline. Values must be files containing YAML/JSON.',
        )
        parser.add_argument(
            '--context-values',
            action='append',
            default=[],
            metavar='KEY=VALUE',
            help='Context key-value pairs to pass to the Function pipeline. Values must be YAML/JSON. Keys take precedence over --context-files.',
        )
        parser.add_argument(
            '--observed-resources', '-o',
            action='append',
            type=pathlib.Path,
            default=[],
            metavar='PATH',
            help='A YAML file or directory of YAML files specifying the observed state of composed resources.'
        )
        parser.add_argument(
            '--extra-resources',
            action='append',
            type=pathlib.Path,
            default=[],
            metavar='PATH',
            help='A YAML file or directory of YAML files specifying required resources (deprecated, use --required-resources).',
        )
        parser.add_argument(
            '--required-resources', '-e',
            action='append',
            type=pathlib.Path,
            default=[],
            metavar='PATH',
            help='A YAML file or directory of YAML files specifying required resources to pass to the Function pipeline.',
        )
        parser.add_argument(
            '--function-credentials',
            action='append',
            type=pathlib.Path,
            default=[],
            metavar='PATH',
            help='A YAML file or directory of YAML files specifying credentials to use for Functions to render the XR.',
        )
        parser.add_argument(
            '--include-full-xr', '-x',
            action='store_true',
            help="Include a direct copy of the input XR's spedc and metadata fields in the rendered output.",
        )
        parser.add_argument(
            '--include-function-results', '-r',
            action='store_true',
            help='Include informational and warning messages from Functions in the rendered output as resources of kind: Result..',
        )
        parser.add_argument(
            '--include-context', '-c',
            action='store_true',
            help='Include the context in the rendered output as a resource of kind: Context.',
        )

    async def run(self):
        if not self.args.composite.is_file():
            print(f"Composite \"{self.args.composite}\" is not a file", file=sys.stderr)
            sys.exit(1)
        composite = protobuf.Yaml(self.args.composite.read_text())
        if composite.apiVersion == 'pythonic.crossplane.io/v1alpha1' and composite.kind == 'Composite':
            if self.args.composition:
                print('Composite type of "composite.pythonic.crossplane.io" does not use "composition" argument', file=sys.stderr)
                sys.exit(1)
            composition = self.create_composition(composite, '')
        else:
            if not self.args.composition:
                print('"composition" argument required', file=sys.stderr)
                sys.exit(1)
            if self.args.composition.is_file():
                composition = protobuf.Yaml(self.args.composition.read_text())
            else:
                composition = self.create_composition(composite, str(self.args.composition))

        request = protobuf.Message(None, 'request', fnv1.RunFunctionRequest.DESCRIPTOR, fnv1.RunFunctionRequest())
        for entry in self.args.context_files:
            key_path = entry.split('=', 1)
            if len(key_path) != 2:
                print(f"Invalid --context-files: {entry}", file=sys.stderr)
                sys.exit(1)
            path = pathlib.Path(key_path[1])
            if not path.is_file():
                print(f"Invalid --context-files {path} is not a file", file=sys.stderr)
                sys.exit(1)
            request.context[key_path[0]] = protobuf.Yaml(path.read_text())
        for entry in self.args.context_values:
            key_value = entry.split('=', 1)
            if len(key_value) != 2:
                print(f"Invalid --context-values: {entry}", file=sys.stderr)
                sys.exit(1)
            request.context[key_value[0]] = protobuf.Yaml(key_value[1])
        request.observed.composite.resource = composite
        for resource in self.collect_resources(self.args.observed_resources):
            name = resource.metadata.annotations['crossplane.io/composition-resource-name']
            if name:
                request.observed.resources[str(name)].resource = resource
        requireds = [resource for resource in self.collect_resources(self.args.required_resources)]
        requireds += [resource for resource in self.collect_resources(self.args.extra_resources)]
        credentials = []
        for credential in self.collect_resources(self.args.function_credentials):
            if credential.apiVersion == 'v1' and credential.kind == 'Secret':
                credentials.append(credential)
        conditions = protobuf.List()
        results = protobuf.List()

        runner = function.FunctionRunner(self.args.debug, self.args.render_unknowns)
        for fn in composition.spec.pipeline:
            if fn.functionRef.name != 'function-pythonic':
                print(f"Only function-pythonic functions can be run: {fn.functionRef.name}", file=sys.stderr)
                sys.exit(1)
            if not fn.input.step:
                fn.input.step = fn.step
            request.input = fn.input
            for fn_credential in fn.credentials:
                if fn_credential.source == 'Secret' and fn_credential.secretRef:
                    for credential in credentials:
                        if credential.metadata.namespace == fn_credential.secretRef.namespace and credential.metadata.name == fn_credential.secretRef.name:
                            data = request.credentials[str(fn_credential.name)].credential_data.data
                            data()
                            for key, value in credential.data:
                                data[key] = protobuf.B64Decode(value)
                            break
                    else:
                        print(f"Step \"{fn.step}\" secret not found: {fn_credential.secretRef.namespace} {fn_credential.secretRef.name}", file=sys.stderr)
                        sys.exit(1)
            requirements = protobuf.Message(None, 'requirements', fnv1.Requirements.DESCRIPTOR, fnv1.Requirements())
            for _ in range(5):
                response = protobuf.Message(None, 'response', fnv1.RunFunctionResponse.DESCRIPTOR, await runner.RunFunction(request._message, None))
                request.desired.resources()
                self.copy_resource(response.desired.composite, request.desired.composite)
                for name, resource in response.desired.resources:
                    self.copy_resource(resource, request.desired.resources[name])
                request.context = response.context
                request.extra_resources()
                request.required_resources()
                if response.requirements == requirements:
                    break
                requirements = response.requirements
                self.fetch_requireds(requireds, requirements.extra_resources, request.extra_resources)
                self.fetch_requireds(requireds, requirements.resources, request.required_resources)
            request.credentials()
            for condition in response.conditions:
                conditions[protobuf.append] = self.create_condition(condition.type, condition.status, condition.reason, condition.message)
            for result in response.results:
                if result.severity == fnv1.Severity.SEVERITY_FATAL:
                    print(f"Pipeline step {fn.step} returned fatal result: {result.message}", file=sys.stderr)
                    sys.exit(1)
                results[protobuf.append] = {
                    'apiVersion': 'render.crossplane.io/v1beta1',
                    'kind': 'Result',
                    'step': fn.step,
                    'severity': fnv1.Severity.Name(result.severity._value),
                    'reason': result.reason,
                    'message': result.message,
                }

        resources = protobuf.List()
        unready = protobuf.List()
        prefix = composite.metadata.labels['crossplane.io/composite']
        if not prefix:
            prefix = composite.metadata.name
        for name, resource in response.desired.resources:
            if resource.ready != fnv1.Ready.READY_TRUE:
                unready[protobuf.append] = name
            resource = resource.resource
            observed = request.observed.resources[name].resource
            if observed:
                for key in ('namespace', 'generateName', 'name'):
                    if observed.metadata[key]:
                        resource.metadata[key] = observed.metadata[key]
            if not resource.metadata.name and not resource.metadata.generateName:
                resource.metadata.generateName = f"{prefix}-"
            if composite.metadata.namespace:
                resource.metadata.namespace = composite.metadata.namespace
            resource.metadata.annotations['crossplane.io/composition-resource-name'] = name
            resource.metadata.labels['crossplane.io/composite'] = prefix
            if composite.metadata.labels['crossplane.io/claim-name'] and composite.metadata.labels['crossplane.io/claim-namespace']:
                resource.metadata.labels['crossplane.io/claim-namespace'] = composite.metadata.labels['crossplane.io/claim-namespace']
                resource.metadata.labels['crossplane.io/claim-name'] = composite.metadata.labels['crossplane.io/claim-name']
            elif composite.spec.claimRef.namespace and composite.spec.claimRef.name:
                resource.metadata.labels['crossplane.io/claim-namespace'] = composite.spec.claimRef.namespace
                resource.metadata.labels['crossplane.io/claim-name'] = composite.spec.claimRef.name
            resource.metadata.ownerReferences[0].controller = True
            resource.metadata.ownerReferences[0].blockOwnerDeletion = True
            resource.metadata.ownerReferences[0].apiVersion = composite.apiVersion
            resource.metadata.ownerReferences[0].kind = composite.kind
            resource.metadata.ownerReferences[0].name = composite.metadata.name
            resource.metadata.ownerReferences[0].uid = ''
            resources[protobuf.append] = resource

        composite = protobuf.Map()
        for name, value in response.desired.composite.resource:
            composite[name] = value
        composite.apiVersion = request.observed.composite.resource.apiVersion
        composite.kind = request.observed.composite.resource.kind
        if self.args.include_full_xr:
            composite.metadata = request.observed.composite.resource.metadata
            if request.observed.composite.resource.spec:
                composite.spec = request.observed.composite.resource.spec
        else:
            if request.observed.composite.resource.metadata.namespace:
                composite.metadata.namespace = request.observed.composite.resource.metadata.namespace
            composite.metadata.name = request.observed.composite.resource.metadata.name
        if response.desired.composite.ready == fnv1.Ready.READY_FALSE:
            condition = self.create_condition('Ready', False, 'Creating')
        elif response.desired.composite.ready == fnv1.Ready.READY_UNSPECIFIED and len(unready):
            condition = self.create_condition('Ready', False, 'Creating', f"Unready resources: {', '.join(str(name) for name in unready)}")
        else:
            condition = self.create_condition('Ready', True, 'Available')
        composite.status.conditions[protobuf.append] = condition
        for condition in conditions:
            if condition['type'] not in ('Ready', 'Synced', 'Healthy'):
                composite.status.conditions[protobuf.append] = condition

        print('---')
        print(str(composite), end='')
        for resource in sorted(resources, key=lambda resource: str(resource.metadata.annotations['crossplane.io/composition-resource-name'])):
            print('---')
            print(str(resource), end='')
        if self.args.include_function_results:
            for result in results:
                print('---')
                print( str(result), end='')
        if self.args.include_context and response.context:
            print('---')
            print(
                str(protobuf.Map(
                    apiVersion = 'render.crossplane.io/v1beta1',
                    kind = 'Context',
                    fields = response.context,
                )),
                end='',
            )

    def create_composition(self, composite, module):
        composition = protobuf.Map()
        composition.apiVersion = 'apiextensions.crossplane.io/v1'
        composition.kind = 'Composition'
        composition.metadata.name = 'function-pythonic-render'
        composition.spec.compositeTypeRef.apiVersion = composite.apiVersion
        composition.spec.compositeTypeRef.kind = composite.kind
        composition.spec.mode = 'Pipeline'
        composition.spec.pipeline[0].step = 'function-pythonic-render'
        composition.spec.pipeline[0].functionRef.name = 'function-pythonic'
        composition.spec.pipeline[0].input.apiVersion = 'pythonic.fn.crossplane.io/v1alpha1'
        composition.spec.pipeline[0].input.kind = 'Composite'
        composition.spec.pipeline[0].input.composite = module
        return composition

    def collect_resources(self, resources):
        files = []
        for resource in resources:
            if resource.is_file():
                files.append(resource)
            elif resource.is_dir():
                for file in resource.iterdir():
                    if file.suffix in ('.yaml', '.yml'):
                        files.append(file)
            else:
                print(f"Specified resource is not a file or a directory: {resource}", file=sys.stderr)
                sys.exit(1)
        for file in files:
            for document in yaml.safe_load_all(file.read_text()):
                yield protobuf.Value(None, None, document)

    def copy_resource(self, source, destination):
        destination.resource = source.resource
        destination.connection_details()
        for key, value in source.connection_details:
            destination.connection_details[key] = value
        destination.ready = source.ready

    def fetch_requireds(self, requireds, selectors, resources):
        for name, selector in selectors:
            items = resources[name].items
            items() # Force this to get created
            for required in requireds:
                if selector.api_version == required.apiVersion and selector.kind == required.kind:
                    if selector.match_name == required.metadata.name:
                        items[protobuf.append].resource = required
                    elif selector.match_labels.labels:
                        for key, value in selector.match_labels.labels:
                            if value != required.metadata.labels[key]:
                                break
                        else:
                            items[protobuf.append].resource = required

    def create_condition(self, type, status, reason, message=None):
        if isinstance(status, protobuf.FieldMessage):
            if status._value == fnv1.Status.STATUS_CONDITION_TRUE:
                status = 'True'
            elif status._value == fnv1.Status.STATUS_CONDITION_FALSE:
                status = 'False'
            else:
                status = 'Unknown'
        elif isinstance(status, bool):
            if status:
                status = 'True'
            else:
                status = 'False'
        elif status is None:
            status = 'Unknown'
        condition = {
            'type': type,
            'status': status,
            'reason': reason,
            'lastTransitionTime': '2026-01-01T00:00:00Z'
        }
        if message:
            condition['message'] = message
        return condition
