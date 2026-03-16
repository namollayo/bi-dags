from datetime import datetime
import logging

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.providers.http.hooks.http import HttpHook
from annual_reports.utils import get_endpoint, get_publications_by_year
from common.models.annual_reports.annual_reports import Journals, Publications
from common.operators.sqlalchemy_operator import sqlalchemy_task
from executor_config import kubernetes_executor_config
from sqlalchemy.sql import func
from tenacity import retry_if_exception_type, stop_after_attempt

current_year = datetime.now().year
years = list(range(2004, current_year + 1))

logger = logging.getLogger(__name__)

@dag(
    start_date=pendulum.today("UTC").add(days=-1),
    schedule="0 0 */15 * *",
)
def annual_reports_publications_dag():
    @task(executor_config=kubernetes_executor_config)
    def fetch_publication_report_count(year, **kwargs):
        endpoint = get_endpoint(key="publications", year=year)
        http_hook = HttpHook(http_conn_id="cds", method="GET")
        logger.info(f"Getting annual reports publications {year}: {endpoint}")

        response = http_hook.run_with_advanced_retry(
            endpoint=endpoint,
            _retry_args={
                "stop": stop_after_attempt(3),
                "retry": retry_if_exception_type(AirflowException),
            },
        )
        publication_report_count, journals = get_publications_by_year(response.content)
        return {year: (publication_report_count, journals)}

    @task(executor_config=kubernetes_executor_config)
    def fetch_thesis_report_count(year, **kwargs):
        http_hook = HttpHook(http_conn_id = "repo", method="GET")
        response = http_hook.run_with_advanced_retry(
            endpoint=f"api/communities/ad8d4abd-9809-4dc3-b4bf-dd403c9d6a8d/records?resource_type=publication%3A%3Apublication-dissertation&publication_date={year}",
            _retry_args={
                "stop": stop_after_attempt(3),
                "retry": retry_if_exception_type(AirflowException),
            }
        )
        data = response.json()
        total_thesis = data.get('hits', {}).get('total', 0)
        logger.info(f"CDS Repo: {year} - {total_thesis}")
        return {year: total_thesis}

    @sqlalchemy_task(conn_id="superset")
    def process_results(results, session, **kwargs):
        for year, values in results.items():
            publications, journals = values
            populate_publication_report_count(publications, year, session)
            populate_journal_report_count(journals, year, session)

    def populate_publication_report_count(publications, year, session, **kwargs):
        full_date_str = f"{year}-01-01"
        year_date = datetime.strptime(full_date_str, "%Y-%m-%d").date()
        record = session.query(Publications).filter_by(year=year_date).first()
        if record:
            record.publications = publications["publications"]
            record.journals = publications["journals"]
            record.contributions = publications["contributions"]
            record.theses = publications["theses"]
            record.rest = publications["rest"]
            record.year = year_date
            record.updated_at = func.now()
        else:
            new_record = Publications(
                year=year_date,
                publications=publications["publications"],
                journals=publications["journals"],
                contributions=publications["contributions"],
                theses=publications["theses"],
                rest=publications["rest"],
            )
            session.add(new_record)

    def populate_journal_report_count(journals, year, session, **kwargs):
        full_date_str = f"{year}-01-01"
        year_date = datetime.strptime(full_date_str, "%Y-%m-%d").date()
        for journal, count in journals.items():
            record = (
                session.query(Journals)
                .filter_by(year=year_date, journal=journal)
                .first()
            )
            if record:
                record.journal = journal
                record.count = count
                record.year = year_date
                record.updated_at = func.now()
            else:
                new_record = Journals(
                    year=year_date,
                    journal=journal,
                    count=count,
                )
                session.add(new_record)

    previous_task = None
    for year in years:
        fetch_task = fetch_publication_report_count.override(
            task_id=f"fetch_report_{year}"
        )(year=year)
        fetch_thesis = fetch_thesis_report_count.override(
            task_id=f"fetch_thesis_{year}"
        )(year=year)
        process_task = process_results.override(task_id=f"process_results_{year}")(
            results=fetch_task,
            thesis_results=fetch_thesis
        )
        if previous_task:
            previous_task >> [fetch_task, fetch_thesis]
        [fetch_task, fetch_thesis] >> process_task
        previous_task = process_task


annual_reports_publications = annual_reports_publications_dag()

