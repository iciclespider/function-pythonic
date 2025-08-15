"""The composition function's main CLI."""

import argparse
import asyncio
import os
import pathlib
import shlex
import signal
import sys
import traceback

import crossplane.function.logging
import crossplane.function.proto.v1.run_function_pb2_grpc as grpcv1
import grpc
import pip._internal.cli.main

from . import function


async def main():
    parser = argparse.ArgumentParser('Forta Crossplane Function')
    parser.add_argument(
        '--debug', '-d',
        action='store_true',
        help='Emit debug logs.',
    )
    parser.add_argument(
        '--address',
        default='0.0.0.0:9443',
        help='Address at which to listen for gRPC connections, default: 0.0.0.0:9443',
    )
    parser.add_argument(
        '--tls-certs-dir',
        default=os.getenv('TLS_SERVER_CERTS_DIR'),
        help='Serve using mTLS certificates.',
    )
    parser.add_argument(
        '--insecure',
        action='store_true',
        help='Run without mTLS credentials. If you supply this flag --tls-certs-dir will be ignored.',
    )
    parser.add_argument(
        '--packages',
        action='store_true',
        help='Discover python packages from function-pythonic ConfigMaps and Secrets.'
    )
    parser.add_argument(
        '--packages-namespace',
        action='append',
        default=[],
        help='Namespaces to discover function-pythonic ConfigMaps and Secrets in, default is cluster wide.',
    )
    parser.add_argument(
        '--pip-install',
        help='Pip install command to install additional Python packages.'
    )
    parser.add_argument(
        '--python-path',
        action='append',
        default=[],
        help='Filing system directories to add to the python path',
    )
    parser.add_argument(
        '--allow-oversize-protos',
        action='store_true',
        help='Allow oversized protobuf messages'
    )
    args = parser.parse_args()

    if args.debug:
        crossplane.function.logging.configure(crossplane.function.logging.Level.DEBUG)
    else:
        crossplane.function.logging.configure(crossplane.function.logging.Level.INFO)

    if args.pip_install:
        pip._internal.cli.main.main(['install', *shlex.split(args.pip_install)])

    # enables read only volumes or mismatched uid volumes
    sys.dont_write_bytecode = True
    for path in reversed(args.python_path):
        sys.path.insert(0, path)

    if args.allow_oversize_protos:
        from google.protobuf.internal import api_implementation
        if api_implementation._c_module:
            api_implementation._c_module.SetAllowOversizeProtos(True)

    grpc.aio.init_grpc_aio()
    grpc_runner = function.FunctionRunner(args.debug)
    grpc_server = grpc.aio.server()
    grpcv1.add_FunctionRunnerServiceServicer_to_server(grpc_runner, grpc_server)
    if args.tls_certs_dir:
        certs = pathlib.Path(args.tls_certs_dir)
        grpc_server.add_secure_port(
            args.address,
            grpc.ssl_server_credentials(
                private_key_certificate_chain_pairs=[(
                    (certs / 'tls.key').read_bytes(),
                    (certs / 'tls.crt').read_bytes(),
                )],
                root_certificates=(certs / 'ca.crt').read_bytes(),
                require_client_auth=True,
            ),
        )
    else:
        if not args.insecure:
            raise ValueError('Either --tls-certs-dir or --insecure must be specified')
        grpc_server.add_insecure_port(args.address)
    await grpc_server.start()

    if args.packages:
        import kopf._core.actions.loggers
        import kopf._core.reactor.running
        from . import packages
        sys.path.insert(0, str(packages.PACKAGES_DIR))
        packages.register_grpc_runner(grpc_runner)
        kopf._core.actions.loggers.configure()
        @kopf.on.startup()
        async def startup(settings, **_):
            settings.scanning.disabled = True
        @kopf.on.cleanup()
        async def cleanup(logger=None, **_):
            await grpc_server.stop(5)
        async with asyncio.TaskGroup() as tasks:
            tasks.create_task(grpc_server.wait_for_termination())
            tasks.create_task(kopf._core.reactor.running.operator(
                standalone=True,
                clusterwide=not args.packages_namespace,
                namespaces=args.packages_namespace,
            ))
    else:
        def stop():
            asyncio.ensure_future(grpc_server.stop(5))
        loop = asyncio.get_event_loop()
        loop.add_signal_handler(signal.SIGINT, stop)
        loop.add_signal_handler(signal.SIGTERM, stop)
        await grpc_server.wait_for_termination()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except:
        print(traceback.format_exc())
        sys.exit(1)
