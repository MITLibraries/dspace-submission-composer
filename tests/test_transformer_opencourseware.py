from dsc.workflows.opencourseware import OpenCourseWareTransformer


def test_opencourseware_transform_success(opencourseware_itemsubmission_source_metadata):
    assert OpenCourseWareTransformer.transform(
        opencourseware_itemsubmission_source_metadata
    ) == {
        "dc.title": "14.02 Principles of Macroeconomics, Fall 2004",
        "dc.date.issued": "2004",
        "dc.description.abstract": (
            "This course provides an overview of the following macroeconomic issues: "
            "the determination of output, employment, unemployment, interest rates, "
            "and inflation. Monetary and fiscal policies are discussed, as are public "
            "debt and international economic issues. This course also introduces basic "
            "models of macroeconomics and illustrates principles with the experience of "
            "the United States and other economies.\n"
        ),
        "dc.contributor.author": ["Caballero, Ricardo"],
        "dc.contributor.department": [
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
        "dc.rights": ("Attribution-NonCommercial-NoDerivs 4.0 United States"),
        "dc.rights.uri": ("https://creativecommons.org/licenses/by-nc-nd/4.0/deed.en"),
        "dc.language.iso": "en_US",
    }


def test_opencourseware_dc_title_success(opencourseware_itemsubmission_source_metadata):
    assert OpenCourseWareTransformer.dc_title(
        opencourseware_itemsubmission_source_metadata
    ) == ("14.02 Principles of Macroeconomics, Fall 2004")


def test_opencourseware_dc_title_if_multi_extra_course_numbers_success(
    opencourseware_itemsubmission_source_metadata,
):
    opencourseware_itemsubmission_source_metadata["extra_course_numbers"] = (
        "14.027J,14.006"
    )

    assert OpenCourseWareTransformer.dc_title(
        opencourseware_itemsubmission_source_metadata
    ) == ("14.02 / 14.027J / 14.006 Principles of Macroeconomics, Fall 2004")


def test_opencourseware_dc_date_issued_success(
    opencourseware_itemsubmission_source_metadata,
):
    assert (
        OpenCourseWareTransformer.dc_date_issued(
            opencourseware_itemsubmission_source_metadata
        )
        == "2004"
    )


def test_opencourseware_dc_description_abstract(
    opencourseware_itemsubmission_source_metadata,
):
    assert isinstance(
        OpenCourseWareTransformer.dc_description_abstract(
            opencourseware_itemsubmission_source_metadata
        ),
        str,
    )


def test_opencourseware_dc_contributor_author_success(
    opencourseware_itemsubmission_source_metadata,
):
    assert OpenCourseWareTransformer.dc_contributor_author(
        opencourseware_itemsubmission_source_metadata
    ) == ["Caballero, Ricardo"]


def test_opencourseware_dc_contributor_author_if_any_names_empty_success(
    opencourseware_itemsubmission_source_metadata,
):
    # the first four entries in the list below result in an empty name ("")
    # only the last entry is included
    opencourseware_itemsubmission_source_metadata["instructors"].extend(
        [
            {},  # all fields missing
            {"middle_initial": "E."},  # all required fields missing
            {"first_name": "Cheese", "middle_initial": "E."},  # "last_name" field missing
            {"last_name": "Burger", "middle_initial": "E."},  # "first_name" field missing
            {"first_name": "Cheese", "last_name": "Burger", "middle_initial": "E."},
        ]
    )
    assert OpenCourseWareTransformer.dc_contributor_author(
        opencourseware_itemsubmission_source_metadata
    ) == [
        "Caballero, Ricardo",
        "Burger, Cheese E.",
    ]


def test_opencourseware_dc_contributor_department_success(
    opencourseware_itemsubmission_source_metadata,
):
    assert opencourseware_itemsubmission_source_metadata["department_numbers"] == ["14"]
    assert OpenCourseWareTransformer.dc_contributor_department(
        opencourseware_itemsubmission_source_metadata
    ) == ["Massachusetts Institute of Technology. Department of Economics"]


def test_opencourseware_creativework_learningresourcetype_success(
    opencourseware_itemsubmission_source_metadata,
):
    assert OpenCourseWareTransformer.creativework_learningresourcetype(
        opencourseware_itemsubmission_source_metadata
    ) == ["Problem Sets with Solutions", "Exams with Solutions", "Lecture Notes"]


def test_opencourseware_dc_subject_success(opencourseware_itemsubmission_source_metadata):
    assert OpenCourseWareTransformer.dc_subject(
        opencourseware_itemsubmission_source_metadata
    ) == [
        "Social Science - Economics - International Economics",
        "Social Science - Economics - Macroeconomics",
    ]


def test_opencourseware_dc_identifier_other_success(
    opencourseware_itemsubmission_source_metadata,
):
    assert OpenCourseWareTransformer.dc_identifier_other(
        opencourseware_itemsubmission_source_metadata
    ) == ["14.02", "14.02-Fall2004"]


def test_opencourseware_dc_coverage_temporal_success(
    opencourseware_itemsubmission_source_metadata,
):
    assert (
        OpenCourseWareTransformer.dc_coverage_temporal(
            opencourseware_itemsubmission_source_metadata
        )
        == "Fall 2004"
    )


def test_opencourseware_dc_audience_educationlevel(
    opencourseware_itemsubmission_source_metadata,
):
    assert OpenCourseWareTransformer.dc_audience_educationlevel(
        opencourseware_itemsubmission_source_metadata
    ) == ["Undergraduate"]
