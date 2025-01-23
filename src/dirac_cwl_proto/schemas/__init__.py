import jsonschema
import pkg_resources
import yaml


def package_loader(uri):
    if uri.startswith("package://"):
        package_path = uri[len("package://") :]
        with pkg_resources.resource_stream(__name__, package_path) as stream:
            return yaml.safe_load(stream)
    raise ValueError(f"Unsupported URI scheme: {uri}")


# Register the custom loader with jsonschema
jsonschema.RefResolver.handlers["package"] = package_loader
