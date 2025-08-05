import yaml
from referencing import Registry
from referencing.exceptions import NoSuchResource


def package_loader(uri):
    if uri.startswith("package://"):
        package_path = uri[len("package://") :]
        # Load the resource using pkg_resources
        from pkg_resources import resource_stream

        with resource_stream(__name__, package_path) as stream:
            return yaml.safe_load(stream)
    raise NoSuchResource(ref=uri)


# Create a Registry with the custom loader
registry = Registry(retrieve=package_loader)  # type: ignore
