import inspect
from collections.abc import Iterable
from typing import Any, ClassVar


class OpenCourseWareTransformer:
    """Transformer for OpenCourseWare (OCW) source metadata."""

    fields: Iterable[str] = [
        # fields with derived values
        "dc_title",
        "dc_date_issued",
        "dc_description_abstract",
        "dc_contributor_author",
        "dc_contributor_department",
        "creativework_learningresourcetype",
        "dc_subject",
        "dc_identifier_other",
        "dc_coverage_temporal",
        "dc_audience_educationlevel",
        # fields with static values
        "dc_type",
        "dc_rights",
        "dc_rights_uri",
        "dc_language_iso",
    ]

    department_mappings: ClassVar = {
        "1": "Massachusetts Institute of Technology. Department of Civil and Environmental Engineering",  # noqa: E501
        "2": "Massachusetts Institute of Technology. Department of Mechanical Engineering",  # noqa: E501
        "3": "Massachusetts Institute of Technology. Department of Materials Science and Engineering",  # noqa: E501
        "4": "Massachusetts Institute of Technology. Department of Architecture",
        "5": "Massachusetts Institute of Technology. Department of Chemistry",
        "6": "Massachusetts Institute of Technology. Department of Electrical Engineering and Computer Science",  # noqa: E501
        "7": "Massachusetts Institute of Technology. Department of Biology",
        "8": "Massachusetts Institute of Technology. Department of Physics",
        "9": "Massachusetts Institute of Technology. Department of Brain and Cognitive Sciences",  # noqa: E501
        "10": "Massachusetts Institute of Technology. Department of Chemical Engineering",
        "11": "Massachusetts Institute of Technology. Department of Urban Studies and Planning",  # noqa: E501
        "12": "Massachusetts Institute of Technology. Department of Earth, Atmospheric, and Planetary Sciences",  # noqa: E501
        "14": "Massachusetts Institute of Technology. Department of Economics",
        "15": "Sloan School of Management",
        "16": "Massachusetts Institute of Technology. Department of Aeronautics and Astronautics",  # noqa: E501
        "17": "Massachusetts Institute of Technology. Department of Political Science",
        "18": "Massachusetts Institute of Technology. Department of Mathematics",
        "20": "Massachusetts Institute of Technology. Department of Biological Engineering",  # noqa: E501
        "21": "Massachusetts Institute of Technology. Department of Humanities",
        "22": "Massachusetts Institute of Technology. Department of Nuclear Science and Engineering",  # noqa: E501
        "24": "Massachusetts Institute of Technology. Department of Linguistics and Philosophy",  # noqa: E501
        "21A": "MIT Anthropology",
        "21E/21S": "Massachusetts Institute of Technology. Department of Humanities and Engineering",  # noqa: E501
        "21G": "MIT Global Languages",
        "21H": "Massachusetts Institute of Technology. History Section",
        "21L": "Massachusetts Institute of Technology. Literature Section",
        "21M": "Massachusetts Institute of Technology. Music and Theater Arts Section",
        "21W": "Massachusetts Institute of Technology. Program in Comparative Media Studies/Writing",  # noqa: E501
        "CMS": "Massachusetts Institute of Technology. Program in Comparative Media Studies/Writing",  # noqa: E501
        "HST": "Harvard University--MIT Division of Health Sciences and Technology",
        "IDS": "Massachusetts Institute of Technology. Institute for Data, Systems, and Society",  # noqa: E501
        "MAS": "Program in Media Arts and Sciences (Massachusetts Institute of Technology)",  # noqa: E501
        "STS": "Massachusetts Institute of Technology. Program in Science, Technology and Society",  # noqa: E501
        "ESD": "Massachusetts Institute of Technology. Engineering Systems Division",
        "WGS": "MIT Program in Women's and Gender Studies",
        "ESG": "MIT Experimental Study Group",
        "EC": "Edgerton Center (Massachusetts Institute of Technology)",
    }

    @classmethod
    def transform(cls, source_metadata: dict) -> dict:
        """Transform source metadata."""
        transformed_metadata: dict[str, Any] = {}

        if not source_metadata:
            return transformed_metadata

        for field in cls.fields:
            field_method = getattr(cls, field)
            formatted_field_name = field.replace("_", ".")

            # check if 'source_metadata' is in signature
            signature = inspect.signature(field_method)
            if "source_metadata" in signature.parameters:
                transformed_metadata[formatted_field_name] = field_method(source_metadata)
            else:
                transformed_metadata[formatted_field_name] = field_method()

        return transformed_metadata

    @classmethod
    def dc_title(cls, source_metadata: dict) -> str:
        """Build a title string from course numbers, title, and term year.

        Examples:
            1. Field 'extra_course_numbers' contains single value
                Input:
                    {
                        "primary_course_number": "6.001",
                        "extra_course_numbers": "18.01",
                        "course_title": "Introduction to Computer Science",
                        "term_year": "2023"
                    }
                Output:
                    "6.001 / 18.01 Introduction to Computer Science, 2023"


            2. Field 'extra_course_numbers' contains multiple values
                Input:
                    {
                        "primary_course_number": "8.01",
                        "extra_course_numbers": "18.01,6.042",
                        "course_title": "Physics I",
                        "term": "Fall",
                        "year": "2021"
                    }
                Output:
                    "8.01 / 18.01 / 6.042 Physics I, Fall 2021"
        """
        title = ""

        # get list of course numbers
        course_numbers: list[str] = []
        course_numbers.append(source_metadata.get("primary_course_number", ""))
        if extra_course_numbers := source_metadata.get("extra_course_numbers", ""):
            course_numbers.extend(extra_course_numbers.split(","))
        course_numbers = [
            course_number for course_number in course_numbers if course_number
        ]

        if course_numbers:
            title += " / ".join(course_numbers)
        if course_title := source_metadata.get("course_title"):
            title += f" {course_title}"
        if term := source_metadata.get("term"):
            title += f", {term}"
        if year := source_metadata.get("year"):
            title += f" {year}"

        return title

    @classmethod
    def dc_date_issued(cls, source_metadata: dict) -> str:
        """Return the year of issue from the source metadata.

        Example:
            Input: {"year": "2022"}
            Output: "2022"
        """
        return source_metadata["year"]

    @classmethod
    def dc_description_abstract(cls, source_metadata: dict) -> str | None:
        """Return the course description from the source metadata.

        Example:
            Input: {"course_description": "An introduction to algorithms."}
            Output: "An introduction to algorithms."
        """
        return source_metadata.get("course_description") or None

    @classmethod
    def dc_contributor_author(cls, source_metadata: dict) -> list[str] | None:
        """Return a list of formatted instructor names.

        Example:
            Input: {"instructors": [{
                        "first_name": "Jane",
                         "last_name": "Doe",
                         "middle_initial": "A."
                        }]
                    }
            Output: ["Doe, Jane A."]
        """
        return [
            instructor_name
            for instructor_details in source_metadata["instructors"]
            if (instructor_name := cls._format_instructor_name(instructor_details))
        ] or None

    @classmethod
    def _format_instructor_name(cls, instructor_details: dict[str, str]) -> str:
        """Format instructor name as 'Last, First Middle'.

        Example:
            Input: {"first_name": "Jane", "last_name": "Doe", "middle_initial": "A."}
            Output: "Doe, Jane A."
        """
        if not (last_name := instructor_details.get("last_name")) or not (
            first_name := instructor_details.get("first_name")
        ):
            return ""
        instructor_name = (
            f"{last_name}, {first_name} {instructor_details.get("middle_initial", "")}"
        )
        return instructor_name.strip()

    @classmethod
    def dc_contributor_department(cls, source_metadata: dict) -> list[str] | None:
        """Return a list of department names mapped from department numbers.

        Example:
            Input: {"department_numbers": ["14", "", "18"]}
            Output:
                [
                    "Massachusetts Institute of Technology. Department of Economics",
                    "Massachusetts Institute of Technology. Department of Mathematics"
                ]
        """
        department_names = [
            cls.department_mappings.get(str(department_number), str(department_number))
            for department_number in source_metadata["department_numbers"]
        ]
        return list(filter(None, department_names)) or None

    @classmethod
    def creativework_learningresourcetype(cls, source_metadata: dict) -> list[str] | None:
        """Return the list of learning resource types.

        Example:
            Input: {"learning_resource_types": ["Lecture Notes", "Exams"]}
            Output: ["Lecture Notes", "Exams"]
        """
        return source_metadata.get("learning_resource_types") or None

    @classmethod
    def dc_subject(cls, source_metadata: dict) -> list[str] | None:
        """Concatenate topic arrays into dash-separated strings.

        Example:
            Input: {"topics": [["Math", "Algebra"], ["Science", "Physics"]]}
            Output: ["Math - Algebra", "Science - Physics"]
        """
        if topics := source_metadata.get("topics"):
            topics_list = [" - ".join(topic_terms) for topic_terms in topics]
            return list(filter(None, topics_list))
        return None

    @classmethod
    def dc_identifier_other(cls, source_metadata: dict) -> list[str] | None:
        """Return a list of course identifiers, including formatted term/year.

        Example:
            Input: {
                "primary_course_number": "6.001",
                "extra_course_numbers": "18.01",
                "term": "Fall",
                "year": "2023"
            }
            Output: ["6.001", "6.001-Fall2023", "18.01"]
        """
        identifier_other_list = []
        if primary_course_number := source_metadata.get("primary_course_number"):
            identifier_other_list.append(primary_course_number)

            # format primary_course_number with term and year
            derived_course_number_parts = [
                primary_course_number,
                f"{source_metadata.get('term', '')}{source_metadata.get('year', '')}",
            ]
            identifier_other_list.append(
                "-".join(part for part in derived_course_number_parts if part)
            )
        if extra_course_numbers := source_metadata.get("extra_course_numbers"):
            identifier_other_list.extend(extra_course_numbers.split(","))
        return identifier_other_list or None

    @classmethod
    def dc_coverage_temporal(cls, source_metadata: dict) -> str | None:
        """Return a string combining term and year.

        Example:
            Input: {"term": "Spring", "year": "2024"}
            Output: "Spring 2024"
        """
        return (
            " ".join(
                [source_metadata.get("term", ""), source_metadata.get("year", "")]
            ).strip()
            or None
        )

    @classmethod
    def dc_audience_educationlevel(cls, source_metadata: dict) -> list[str] | None:
        """Return the education level from the source metadata.

        Example:
            Input: {"level": ["Undergraduate"]}
            Output: "Undergraduate"
        """
        return source_metadata.get("level") or None

    @classmethod
    def dc_type(cls) -> str:
        return "Learning Object"

    @classmethod
    def dc_rights(cls) -> str:
        return "Attribution-NonCommercial-NoDerivs 4.0 United States"

    @classmethod
    def dc_rights_uri(cls) -> str:
        return "https://creativecommons.org/licenses/by-nc-nd/4.0/deed.en"

    @classmethod
    def dc_language_iso(cls) -> str:
        return "en_US"
