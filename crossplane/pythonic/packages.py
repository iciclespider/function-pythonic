
import base64
import logging
import pathlib
import sys

import kopf


GRPC_SERVER = None
GRPC_RUNNER = None
PACKAGES_DIR = None
PACKAGE_LABEL = {'function-pythonic.package': kopf.PRESENT}


def operator(grpc_server, grpc_runner, packages_secrets, packages_namespaces, packages_dir):
    logging.getLogger('kopf.objects').setLevel(logging.INFO)
    global GRPC_SERVER, GRPC_RUNNER, PACKAGES_DIR
    GRPC_SERVER = grpc_server
    GRPC_RUNNER = grpc_runner
    PACKAGES_DIR = pathlib.Path(packages_dir).expanduser().resolve()
    sys.path.insert(0, str(PACKAGES_DIR))
    if packages_secrets:
        kopf.on.create('', 'v1', 'secrets', labels=PACKAGE_LABEL)(create)
        kopf.on.resume('', 'v1', 'secrets', labels=PACKAGE_LABEL)(create)
        kopf.on.update('', 'v1', 'secrets', labels=PACKAGE_LABEL)(update)
        kopf.on.delete('', 'v1', 'secrets', labels=PACKAGE_LABEL)(delete)
    return kopf.operator(
        standalone=True,
        clusterwide=not packages_namespaces,
        namespaces=packages_namespaces,
    )


@kopf.on.startup()
async def startup(settings, **_):
    settings.scanning.disabled = True


@kopf.on.cleanup()
async def cleanup(**_):
    await GRPC_SERVER.stop(5)


@kopf.on.create('', 'v1', 'configmaps', labels=PACKAGE_LABEL)
@kopf.on.resume('', 'v1', 'configmaps', labels=PACKAGE_LABEL)
async def create(body, logger, **_):
    package_dir = get_package_dir(body, logger)
    if package_dir:
        secret = body['kind'] == 'Secret'
        for name, text in body.get('data', {}).items():
            package_file_write(package_dir, name, secret, text, 'Created', logger)


@kopf.on.update('', 'v1', 'configmaps', labels=PACKAGE_LABEL)
async def update(body, old, logger, **_):
    old_package_dir = get_package_dir(old)
    if old_package_dir:
        old_data = old.get('data', {})
    else:
        old_data = {}
    old_names = set(old_data.keys())
    package_dir = get_package_dir(body, logger)
    if package_dir:
        secret = body['kind'] == 'Secret'
        for name, text in body.get('data', {}).items():
            if package_dir == old_package_dir and text == old_data.get(name, None):
                action = 'Unchanged'
            else:
                action = 'Updated' if package_dir == old_package_dir and name in old_names else 'Created'
            package_file_write(package_dir, name, secret, text, action, logger)
            if package_dir == old_package_dir:
                old_names.discard(name)
    if old_package_dir:
        for name in old_names:
            package_file_unlink(old_package_dir, name, 'Removed', logger)


@kopf.on.delete('', 'v1', 'configmaps', labels=PACKAGE_LABEL)
async def delete(old, logger, **_):
    package_dir = get_package_dir(old)
    if package_dir:
        for name in old.get('data', {}).keys():
            package_file_unlink(package_dir, name, 'Deleted', logger)


def get_package_dir(body, logger=None):
    package = body.get('metadata', {}).get('labels', {}).get('function-pythonic.package', None)
    if package is None:
        if logger:
            logger.error('function-pythonic.package label is missing')
        return None
    package_dir = PACKAGES_DIR
    if package:
        for segment in package.split('.'):
            if not segment.isidentifier():
                if logger:
                    logger.error('Package has invalid package name: %s', package)
                return None
            package_dir = package_dir / segment
    return package_dir


def package_file_write(package_dir, name, secret, text, action, logger):
    package_file = package_dir / name
    if action != 'Unchanged':
        package_file.parent.mkdir(parents=True, exist_ok=True)
        if secret:
            package_file.write_bytes(base64.b64decode(text.encode('utf-8')))
        else:
            package_file.write_text(text)
    module, name = package_file_name(package_file)
    if module:
        if action != 'Unchanged':
            GRPC_RUNNER.invalidate_module(name)
        logger.info(f"{action} module: {name}")
    else:
        logger.info(f"{action} file: {name}")


def package_file_unlink(package_dir, name, action, logger):
    package_file = package_dir / name
    package_file.unlink(missing_ok=True)
    module, name = package_file_name(package_file)
    if module:
        GRPC_RUNNER.invalidate_module(name)
        logger.info(f"{action} module: {name}")
    else:
        logger.info(f"{action} file: {name}")
    package_dir = package_file.parent
    while (
            package_dir.is_relative_to(PACKAGES_DIR)
            and package_dir.is_dir()
            and not list(package_dir.iterdir())
    ):
        package_dir.rmdir()
        module = str(package_dir.relative_to(PACKAGES_DIR)).replace('/', '.')
        if module != '.':
            GRPC_RUNNER.invalidate_module(module)
            logger.info(f"{action} package: {module}")
        package_dir = package_dir.parent


def package_file_name(package_file):
    name = str(package_file.relative_to(PACKAGES_DIR))
    if name.endswith('.py'):
        return True, name[:-3].replace('/', '.')
    return False, name
