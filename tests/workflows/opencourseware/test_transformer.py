import json
import zipfile

import pytest

from dsc.workflows.opencourseware import OpenCourseWareTransformer


@pytest.fixture(autouse=True)
def opencourseware_source_metadata():
    with (
        open("tests/fixtures/opencourseware/123.zip", "rb") as file_input,
        zipfile.ZipFile(file_input) as zip_file,
        zip_file.open("data.json") as json_file,
    ):
        yield json.load(json_file)


def test_opencourseware_transformer_success(opencourseware_source_metadata):
    assert OpenCourseWareTransformer.transform(opencourseware_source_metadata) == {
        "dc.title": "14.02 Principles of Macroeconomics, Fall 2004",
        "dc.date.issued": "2004",
        "dc.description.abstract": (
            "This course provides an overview of the following macroeconomic "
            "issues: the determination of output, employment, unemployment, "
            "interest rates, and inflation. Monetary and fiscal policies are "
            "discussed, as are public debt and international economic issues. "
            "This course also introduces basic models of macroeconomics and "
            "illustrates principles with the experience of the United States "
            "and other economies.\n"
        ),
        "dc.contributor.author": ["Caballero, Ricardo"],
        "dc.relation.orgunit": [
            "Massachusetts Institute of Technology. Department of Economics"
        ],
        "creativework.learningresourcetype": [
            "Problem Sets with Solutions",
            "Exams with Solutions",
            "Lecture Notes",
        ],
        "dc.subject": [
            "Social Science - Economics - International Economics",
            "Social Science - Economics - Macroeconomics",
        ],
        "dc.identifier.other": ["14.02", "14.02-Fall2004"],
        "dc.coverage.temporal": "Fall 2004",
        "dc.audience.educationlevel": ["Undergraduate"],
        "dc.type": "Learning Object",
        "dc.rights": "Attribution-NonCommercial-NoDerivs 4.0 United States",
        "dc.rights.uri": "https://creativecommons.org/licenses/by-nc-nd/4.0/deed.en",
        "dc.language.iso": "en_US",
    }
