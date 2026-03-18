
import base64
import logging
import pathlib
import sys

import kopf


GRPC_SERVER = None
GRPC_RUNNER = None
PACKAGES_DIR = None
PACKAGE_LABEL = {'function-pythonic.package': kopf.PRESENT}


def operator(grpc_server, grpc_runner, packages_configmaps, packages_secrets, packages_namespaces, packages_environmentconfigs, packages_compositions, packages_dir):
    logging.getLogger('kopf.objects').setLevel(logging.INFO)
    global GRPC_SERVER, GRPC_RUNNER, PACKAGES_DIR
    GRPC_SERVER = grpc_server
    GRPC_RUNNER = grpc_runner
    PACKAGES_DIR = pathlib.Path(packages_dir).expanduser().resolve()
    sys.path.insert(0, str(PACKAGES_DIR))
    if packages_configmaps:
        on_resource('', 'v1', 'configmaps')
    if packages_secrets:
        on_resource('', 'v1', 'secrets')
    if not packages_namespaces:
        if packages_environmentconfigs:
            on_resource('apiextensions.crossplane.io', 'v1beta1', 'environmentconfigs')
        if packages_compositions:
            on_resource('apiextensions.crossplane.io', 'v1', 'compositions')
    return kopf.operator(
        standalone=True,
        clusterwide=not packages_namespaces,
        namespaces=packages_namespaces,
    )

def on_resource(group, version, plural):
    kopf.on.create(group, version, plural, labels=PACKAGE_LABEL)(create)
    kopf.on.resume(group, version, plural, labels=PACKAGE_LABEL)(create)
    kopf.on.update(group, version, plural, labels=PACKAGE_LABEL)(update)
    kopf.on.delete(group, version, plural, labels=PACKAGE_LABEL)(delete)


@kopf.on.startup()
async def startup(settings, **_):
    settings.scanning.disabled = True


@kopf.on.cleanup()
async def cleanup(**_):
    await GRPC_SERVER.stop(5)


async def create(body, logger, **_):
    resource_create('Created', body, logger)


async def update(body, old, logger, **_):
    resource_delete('Removed', old, logger)
    resource_create('Added', body, logger)


async def delete(body, logger, **_):
    resource_delete('Deleted', body, logger)


def resource_create(action, body, logger):
    package_dir = get_package_dir(body, logger)
    if not package_dir:
        return
    kind = body['kind']
    if kind in ('ConfigMap', 'Secret', 'EnvironmentConfig'):
        package_create(kind, action, package_dir, body.get('data', {}), logger)
    elif kind == 'Composition':
        for step in body.get('spec', {}).get('pipeline', []):
            input = step.get('input')
            if input and input.get('apiVersion') == 'pythonic.fn.crossplane.io/v1alpha1':
                package_create(kind, action, package_dir, input.get('packages', {}), logger)


def resource_delete(action, body, logger):
    package_dir = get_package_dir(body, logger)
    if not package_dir:
        return
    kind = body['kind']
    if kind in ('ConfigMap', 'Secret', 'EnvironmentConfig'):
        package_delete(action, package_dir, body.get('data', {}), logger)
    elif kind == 'Composition':
        for step in body.get('spec', {}).get('pipeline', []):
            input = step.get('input')
            if input and input.get('apiVersion') == 'pythonic.fn.crossplane.io/v1alpha1':
                package_delete(action, package_dir, step.get('input', {}).get('packages', {}), logger)


def package_create(kind, action, package_dir, package, logger):
    for name, value in package.items():
        if validate_entry(name, value, logger):
            package_name = package_dir / name
            if isinstance(value, str):
                package_name.parent.mkdir(parents=True, exist_ok=True)
                if kind == 'Secret':
                    package_name.write_bytes(base64.b64decode(value.encode('utf-8')))
                else:
                    package_name.write_text(value)
                module, name = package_file_name(package_name)
                if module:
                    GRPC_RUNNER.invalidate_module(name)
                    logger.info(f"{action} module: {name}")
                else:
                    logger.info(f"{action} file: {name}")
            elif isinstance(value, dict):
                package_create(kind, action, package_name, value, logger)


def package_delete(action, package_dir, package, logger):
    for name, value in package.items():
        if validate_entry(name, value, logger):
            package_name = package_dir / name
            if isinstance(value, str):
                package_name.unlink(missing_ok=True)
                module, name = package_file_name(package_name)
                if module:
                    GRPC_RUNNER.invalidate_module(name)
                    logger.info(f"{action} module: {name}")
                else:
                    logger.info(f"{action} file: {name}")
                parent = package_name.parent
                while (
                        parent.is_relative_to(PACKAGES_DIR)
                        and parent.is_dir()
                        and not list(parent.iterdir())
                ):
                    parent.rmdir()
                    module = str(parent.relative_to(PACKAGES_DIR)).replace('/', '.')
                    if module != '.':
                        GRPC_RUNNER.invalidate_module(module)
                        logger.info(f"{action} package: {module}")
                    parent = parent.parent
            elif isinstance(value, dict):
                package_delete(action, package_name, value, logger)


def validate_entry(name, value, logger):
    if isinstance(value, str):
        if not name.endswith('.py'):
            if '.' in name or '/' in name:
                logger.error(f"Python package file name is not valid: {name}")
                return False
            return True
        name = name[:-3]
    elif not isinstance(value, dict):
        logger.error(f"Python package \"{name}\" value is not a valid type: {value.__class__}")
        return False
    if name.isidentifier():
        return True
    logger.error(f"Python package name is not an identifier: {name}")
    return False


def get_package_dir(body, logger):
    package = body.get('metadata', {}).get('labels', {}).get('function-pythonic.package')
    if package is None:
        if logger:
            logger.error('function-pythonic.package label is missing')
        return None
    package_dir = PACKAGES_DIR
    if body['kind'] in ('ConfigMap', 'Secret') and package:
        for segment in package.split('.'):
            if not segment.isidentifier():
                logger.error('Package has invalid package name: %s', package)
                return None
            package_dir = package_dir / segment
    return package_dir


def package_file_name(package_name):
    name = str(package_name.relative_to(PACKAGES_DIR))
    if name.endswith('.py'):
        return True, name[:-3].replace('/', '.')
    return False, name
