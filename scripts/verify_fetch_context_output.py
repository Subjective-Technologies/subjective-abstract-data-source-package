import os
import re
import tempfile

from brainboost_configuration_package.BBConfig import BBConfig

from subjective_abstract_data_source_package.SubjectiveDataSource import SubjectiveDataSource


class SubjectiveFetchDataSource(SubjectiveDataSource):
    def fetch(self):
        return {"demo": True}

    def get_icon(self):
        return "<svg></svg>"

    def get_connection_data(self):
        return {"connection_type": "Mock", "fields": []}


def main():
    with tempfile.TemporaryDirectory() as temp_dir:
        config_path = os.path.join(temp_dir, "bbconfig_demo.config")
        with open(config_path, "w", encoding="utf-8") as f:
            f.write("CONTEXT_STORAGE=" + temp_dir + "\n")
            f.write("CONTEXT_OUTPUT_FORMAT=json\n")
            f.write("CONTEXT_FILE_NAME_CONVENTION=YYYY_MM_DD_HH_MM_SS-[ds_name]-context.${CONTEXT_OUTPUT_FORMAT}\n")

        BBConfig.configure(config_path)

        ds = SubjectiveFetchDataSource(params={})
        ds.fetch()

        files = os.listdir(temp_dir)
        ds_name = ds.get_data_source_type_name()
        pattern = re.compile(r"^\d{4}_\d{2}_\d{2}_\d{2}_\d{2}_\d{2}-" + re.escape(ds_name) + r"-context\.json$")
        matches = [name for name in files if pattern.match(name)]

        if len(matches) != 1:
            print("Expected one context file, found:", files)
            return 1

        print("Created context file:", matches[0])
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
