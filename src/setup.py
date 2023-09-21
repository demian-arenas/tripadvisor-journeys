import boto3
import dotenv
import pandas as pd
import os
import duckdb


def get_env(key):
    """
    Get the value of the specified key from the environment variables.

    Args:
        key (str): The key to retrieve the value for.

    Returns:
        str: The value of the specified key from the environment variables.
    """
    keys = dotenv.dotenv_values()
    return keys[key]


def create_sequence_tables():
    # connect to the database
    duck_con = duckdb.connect(get_env("DUCKDB_DATABASE"))

    duck_con.sql(f"""INSTALL parquet;""")
    duck_con.sql(f"""SET memory_limit='16GB';""")
    duck_con.sql(f"SET enable_progress_bar=true;")
    duck_con.execute("""DROP TABLE IF EXISTS user_sequence;""")
    duck_con.execute("""CREATE TABLE IF NOT EXISTS user_sequence AS WITH prebase_first_visit AS (
        SELECT DISTINCT userid,
            eventdate,
            date_casted,
            event_order,
            (date_casted - '5 days'::interval) AS session_before
        FROM clickstream2
        WHERE visit_tripadvisor = TRUE
            AND visit_number = 1
            AND have_problem_with_timestamp = FALSE
        ORDER BY eventdate
    ),
    data_before_first_visit AS (
        SELECT clickstream2.userid,
            clickstream2.date_casted,
            clickstream2.event_order,
            clickstream2.referrerurl previous_url,
            clickstream2.targeturl current_url,
            LEAD(clickstream2.targeturl) OVER (
                PARTITION BY clickstream2.userid
                ORDER BY clickstream2.event_order
            ) AS next_target_url
        FROM clickstream2
            INNER JOIN prebase_first_visit ON clickstream2.userid = prebase_first_visit.userid
        WHERE clickstream2.event_order < prebase_first_visit.event_order
            AND clickstream2.eventdate >= prebase_first_visit.session_before
            AND clickstream2.have_problem_with_timestamp = FALSE
    ),
    filtered_data AS (
        SELECT userid,
            date_casted,
            event_order,
            previous_url,
            current_url
        FROM data_before_first_visit
        WHERE current_url != next_target_url
            OR next_target_url IS NULL
    )
    SELECT userid,
        date_casted,
        current_url AS url,
        ROW_NUMBER() OVER (
            PARTITION BY userid
            ORDER BY event_order
        ) AS sequence,
        ROW_NUMBER() OVER (
            PARTITION BY userid
            ORDER BY event_order DESC
        ) AS reverse_sequence
    FROM filtered_data
    ORDER BY userid,
        sequence;"""
    )
    print("user_sequence table created.")

    duck_con.execute("""DROP TABLE IF EXISTS user_sequence_domains;""")
    duck_con.execute("""CREATE TABLE IF NOT EXISTS user_sequence_domains AS WITH prebase_first_visit AS (
        SELECT DISTINCT userid,
            eventdate,
            date_casted,
            event_order,
            (date_casted - '5 days'::interval) AS session_before
        FROM clickstream2
        WHERE visit_tripadvisor = TRUE
            AND visit_number = 1
            AND clickstream2.have_problem_with_timestamp = FALSE
        ORDER BY eventdate
    ),
    domain_extraction AS (
        SELECT *
        FROM (
            SELECT clickstream2.userid,
            clickstream2.event_order,
            clickstream2.date_casted,
            regexp_extract(referrerurl, '^(?:https?:\/\/)?(?:[^@\/\n]+@)?(?:www\.)?([^:\/\n]+)', 1) AS previous_domain,
            regexp_extract(targeturl, '^(?:https?:\/\/)?(?:[^@\/\n]+@)?(?:www\.)?([^:\/\n]+)', 1) AS current_domain
        FROM clickstream2
            INNER JOIN prebase_first_visit p ON clickstream2.userid = p.userid
        WHERE clickstream2.event_order < p.event_order
            AND clickstream2.date_casted >= p.session_before
            AND clickstream2.have_problem_with_timestamp = FALSE
        ) as subquery
        WHERE current_domain != ''
        AND current_domain IS NOT NULL
    ),
    data_before_first_visit AS (
        SELECT d.userid,
            d.date_casted,
            d.event_order,
            d.previous_domain,
            d.current_domain,
            LEAD(d.current_domain) OVER (
                PARTITION BY d.userid
                ORDER BY d.event_order
            ) AS next_domain
        FROM domain_extraction d
    ),
    filtered_data AS (
        SELECT userid,
            date_casted,
            event_order,
            previous_domain,
            current_domain
        FROM data_before_first_visit
        WHERE current_domain != next_domain
        OR next_domain IS NULL
    )
    SELECT userid,
        date_casted,
        current_domain AS domain,
        ROW_NUMBER() OVER (
            PARTITION BY userid
            ORDER BY event_order
        ) AS sequence,
        ROW_NUMBER() OVER (
            PARTITION BY userid
            ORDER BY event_order DESC
        ) AS reverse_sequence
    FROM filtered_data
    ORDER BY userid,
        sequence"""
    )
    print("user_sequence_domains table created.")

    duck_con.execute("""UPDATE user_sequence_domains
    SET domain = CASE
            WHEN LENGTH(domain) - LENGTH(REPLACE(domain, '.', '')) >= 2 THEN
                CONCAT(
                    REVERSE(SUBSTRING(
                        REVERSE(domain),
                        position('.' in REVERSE(domain)) + 1,
                        position('.' in SUBSTRING(REVERSE(domain), position('.' in REVERSE(domain)) + 1)) -1 
                    )),
                    REVERSE(SUBSTRING(
                        REVERSE(domain),
                        1,
                        position('.' in REVERSE(domain))
                    ))
                )
            ELSE domain
        END
    WHERE LENGTH(domain) - LENGTH(REPLACE(domain, '.', '')) >= 2;
    """)

    print("user_sequence_domains table updated.")

    websites_categories = {
        "Search Engines": ["google.com", "bing.com", "duckduckgo.com", "yahoo.com", "ecosia.org"],
        "Social Media": ["facebook.com", "instagram.com", "twitter.com", "tumblr.com", "linkedin.com", "pinterest.com", "tiktok.com", "messenger.com", "discord.com", "whatsapp.com", "reddit.com", "snapchat.com"],
        "Streaming & Entertainment": ["youtube.com", "netflix.com", "hulu.com", "hbomax.com", "spotify.com", "vimeo.com"],
        "E-commerce": ["amazon.com", "ebay.com", "walmart.com", "etsy.com", "aliexpress.com", "bestbuy.com", "lowes.com", "kohls.com", "macys.com", "hotels.com", "rakuten.com", "wayfair.com", "gap.com", "shein.com"],
        "News & Media": ["nytimes.com", "cnn.com", "foxnews.com", "washingtonpost.com", "wsj.com", "usatoday.com", "bbc.com", "cnbc.com", "nypost.com", "forbes.com", "theguardian.com", "businessinsider.com", "buzzfeed.com"],
        "Technology & Web Services": ["microsoft.com", "apple.com", "github.com", "adobe.com", "slack.com", "salesforce.com", "dropbox.com", "godaddy.com", "cloudfront.net", "wix.com"],
        "Education": ["wikipedia.org", "instructure.com", "quizlet.com", "blackboard.com", "chegg.com", "harvard.edu", "ucdavis.edu", "usc.edu", "stanford.edu", "rutgers.edu", "umich.edu", "stanford.edu", "columbia.edu", "cuny.edu", "ufl.edu", "wisc.edu", "ucf.edu", "pitt.edu", "washington.edu", "duke.edu", "yale.edu", "northwestern.edu", "miami.edu", "wustl.edu", "utexas.edu", "nyu.edu", "arizona.edu", "studentdoctor.net", "squarespace.com", "ucla.edu", "upenn.edu", "umn.edu"],
        "Health": ["nih.gov", "mayoclinic.org", "webmd.com", "cdc.gov"],
        "Finance & Banking": ["chase.com", "bankofamerica.com", "paypal.com", "americanexpress.com", "fidelity.com", "wellsfargo.com", "capitalone.com", "intuit.com", "citi.com", "vanguard.com", "schwab.com", "robinhood.com"],
        "Business & Productivity": ["office.com", "zoom.us", "slack.com", "salesforce.com", "glassdoor.com"],
        "Ads & Marketing": ["doubleclick.net", "googlesyndication.com", "googleadservices.com"],
        "Online Communities & Forums": ["quora.com", "fandom.com", "medium.com", "stackexchange.com", "reddit.com", "studentdoctor.net"],
        "Travel & Accommodation": ["expedia.com", "airbnb.com", "booking.com", "priceline.com", "kayak.com", "flyfrontier.com", "delta.com"],
        "Government": ["irs.gov", "state.gov", "uscis.gov", "va.gov"],
    }


    # Create a table with the categories
    duck_con.execute("""DROP TABLE IF EXISTS websites_categories""")
    duck_con.execute("""DROP SEQUENCE IF EXISTS website_id""")

    duck_con.sql("""CREATE SEQUENCE website_id START 1;""")

    duck_con.execute("""CREATE TABLE IF NOT EXISTS websites_categories (
        id INT DEFAULT NEXTVAL('website_id'),
        domain VARCHAR(255) NOT NULL,
        category VARCHAR(255) NOT NULL
    );
    """)

    for category, domains in websites_categories.items():
        for domain in domains:
            duck_con.execute(f"""INSERT INTO websites_categories (domain, category) VALUES ('{domain}', '{category}');""")

    print("websites_categories table created.")

    duck_con.close()


def check_if_parquets_are_downloaded(filedir="./data/"):
    """
    Check if the parquets are downloaded
    """
    if len(os.listdir(filedir)) == 48:
        return True
    else:
        return False


def download_parquets_from_s3(filedir="./data/"):
    """
    Downloads all parquet files from an S3 bucket and saves them to a local directory.

    Args:
        filedir (str): The directory to save the downloaded files. Default is './data/'.

    Returns:
        None
    """
    S3_REGION = get_env("S3_REGION")
    S3_ACCESS_KEY = get_env("S3_ACCESS_KEY")
    S3_SECRET_KEY = get_env("S3_SECRET_KEY")
    S3_BUCKET = get_env("S3_BUCKET")

    session = boto3.Session(
        aws_access_key_id=S3_ACCESS_KEY, aws_secret_access_key=S3_SECRET_KEY
    )

    s3 = session.resource("s3")

    bucket = s3.Bucket(S3_BUCKET)
    for obj in bucket.objects.all():
        file_name = obj.key
        # remove clickstream2/ from the file name
        file_name = file_name.replace("clickstream2/", "")

        # path to save the file is data/
        file_name = "data/" + file_name

        # download the file
        bucket.download_file(obj.key, file_name)


def load_parquets_to_database(filenames="./data/*.parquet"):
    """
    Load parquet files from the specified directory into a DuckDB database.

    Args:
        filenames (str): The path to the directory containing the parquet files to be loaded. Defaults to './data/*.parquet'.

    Returns:
        None
    """

    # connect to the database
    conn = duckdb.connect(get_env("DUCKDB_DATABASE"))

    conn.sql(f"""INSTALL parquet;""")
    conn.sql(f"""SET memory_limit='16GB';""")
    conn.sql(f"SET enable_progress_bar=true;")
    conn.sql(f"""LOAD parquet;""")

    conn.sql(f"""DROP TABLE IF EXISTS clickstream2;""")

    conn.sql("""CREATE SEQUENCE event_id START 1;""")

    conn.sql(
        f"""CREATE TABLE IF NOT EXISTS clickstream2 (
            id INTEGER DEFAULT NEXTVAL('event_id'),
             userid VARCHAR(80),
             eventdate DATE(),
             eventtimestamp UINTEGER,
             countrycode VARCHAR(10),
             city VARCHAR(20),
             postalcode VARCHAR(5),
             platform VARCHAR(8),
             referrerurl VARCHAR(4020),
             targeturl VARCHAR(4020),
             datasetcode VARCHAR(10),
             httpcode VARCHAR(3)
             );"""
    )
    print("Database table created.")

    for file in os.listdir("./data/"):
        if file.endswith(".parquet"):
            # Insert data, if already loaded updated the data
            conn.sql(
                f"""INSERT INTO clickstream2
                BY NAME SELECT * FROM parquet_scan('./data/{file}');"""
            )
            print(f"{file} loaded to database.")

    print("Parquet files loaded to database.")

    # Create column called date_casted from eventtimestamp column using to_timestamp
    conn.execute("ALTER TABLE clickstream2 ADD COLUMN IF NOT EXISTS date_casted DATE")
    conn.execute(
        "UPDATE clickstream2 SET date_casted = CAST(to_timestamp(eventtimestamp) AS DATE)"
    )
    print("date_casted column created.")

    # Create a column called is_outlier when date_casted is not equal to eventdate with 1 day difference
    conn.execute(
        "ALTER TABLE clickstream2 ADD COLUMN IF NOT EXISTS have_problem_with_timestamp BOOLEAN"
    )
    conn.execute(
        """UPDATE clickstream2
                 SET have_problem_with_timestamp = CASE WHEN eventdate < (date_casted - 1) OR eventdate > (date_casted + 1) THEN TRUE ELSE FALSE END"""
    )

    # Enumerate each event for each user using ROW_NUMBER() OVER (PARTITION BY userid ORDER BY eventtimestamp)
    conn.execute(
        "ALTER TABLE clickstream2 ADD COLUMN IF NOT EXISTS event_order INTEGER"
    )

    # Update the event_order column. window functions are not allowed in UPDATE
    conn.execute(
        """UPDATE clickstream2
                        SET event_order = subquery.event_order
                        FROM (
                            SELECT id, ROW_NUMBER() OVER (PARTITION BY userid ORDER BY eventtimestamp) AS event_order
                            FROM clickstream2
                            WHERE have_problem_with_timestamp = FALSE
                        ) AS subquery
                        WHERE clickstream2.id = subquery.id"""
    )

    # add a column with value 1 each time the user visits tripadvisor
    conn.execute(
        """ALTER TABLE clickstream2 ADD COLUMN IF NOT EXISTS visit_tripadvisor BOOLEAN"""
    )

    conn.execute(
        """UPDATE clickstream2
                        SET visit_tripadvisor = CASE WHEN targeturl LIKE '%.tripadvisor%' THEN TRUE ELSE FALSE END"""
    )

    # add a column with the number of times the user visited tripadvisor, only count when visit_tripadvisor is 1, otherwise set 0
    conn.execute(
        """ALTER TABLE clickstream2 ADD COLUMN IF NOT EXISTS visit_number INTEGER"""
    )
    conn.execute(
        """UPDATE clickstream2 SET visit_number = subquery.visit_number
                        FROM (
                            SELECT id, ROW_NUMBER() OVER (PARTITION BY userid ORDER BY eventtimestamp) AS visit_number
                            FROM clickstream2
                            WHERE visit_tripadvisor = TRUE
                            AND have_problem_with_timestamp = FALSE
                        ) AS subquery
                        WHERE clickstream2.id = subquery.id"""
    )

    # Add a column to count number of days that the user visited tripadvisor
    conn.execute(
        """ALTER TABLE clickstream2 ADD COLUMN IF NOT EXISTS day_visited_number INTEGER"""
    )
    conn.execute(
        """UPDATE clickstream2 SET day_visited_number = subquery.day_visited_number
                        FROM (
                            SELECT userid, date_casted, ROW_NUMBER() OVER (PARTITION BY userid ORDER BY date_casted) AS day_visited_number
                            FROM clickstream2
                            WHERE visit_tripadvisor = TRUE
                            AND have_problem_with_timestamp = FALSE
                            GROUP BY userid, date_casted
                        ) AS subquery
                        WHERE clickstream2.userid = subquery.userid AND clickstream2.date_casted = subquery.date_casted"""
    )

    conn.close()


if __name__ == "__main__":
    # check if the parquets are downloaded
    if not check_if_parquets_are_downloaded():
    #     # download the parquets
        download_parquets_from_s3()

    # # load the parquets to the database
    load_parquets_to_database()

    # create user sequence tables
    create_sequence_tables()
