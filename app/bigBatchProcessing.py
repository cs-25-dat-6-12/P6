import time
import openai
import json
import os
import winsound
import csv
from datetime import datetime
from pathlib import Path

# process batch files that exceed the request or file size limits imposed by OpenAI
# that's files that exceed 100MB or 50.000 requests.


class ValidationFailedException(Exception):
    pass


def utf8len(string):
    # return the number of bytes in a string encoded with utf-8
    return len(string.encode("utf-8"))


def dict_list_from_csv(filepath):
    # given a csv file, create a list of dictionaries corresponding to each row with column names as keys
    with open(filepath, "r") as file:
        dict_reader = csv.DictReader(file)
        return list(dict_reader)


def dict_list_to_csv(filepath, dict_list):
    # given a list of dictionaries of the format returned by dict_list_from_csv, write a csv with those dicts as rows
    # assumes all dictionaries in the list have the same keys!
    with open(filepath, "w", newline="") as file:
        keys = dict_list[0].keys()
        dict_writer = csv.DictWriter(file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(dict_list)


def ticker_string_sleep(prefix, duration, suffix):
    # prints a string with a built-in countdown for the specified duration (in seconds)
    for i in range(duration):
        string = prefix + str(duration - i) + suffix + " "
        print(string, end="\r")
        time.sleep(1)


def split_jsonl(src_filepath, dst_directory, size_limit_MB=100, request_limit=50000):
    # given the path of a jsonl-file, split it according to the specified size limits and create a number of smaller jsonl-files in the given directory
    size_limit = size_limit_MB * 1000000  # convert MB to Bytes

    # getting the filename for easier organization of experiments
    src_filename = Path(src_filepath).stem

    with open(src_filepath, "r") as main_file:
        # create the directory we need to put our subfiles into
        try:
            os.makedirs(dst_directory)
        except FileExistsError:
            if os.listdir(dst_directory):
                # NOTE to save time, we skip the splitting if there's already files at our destination, since we probably already split
                print("Files present in target directory already. Skipping split step.")
                return
            else:
                print(
                    "Empty target directory already exists. Make sure you know what you're doing."
                )

        subfiles_count = 1
        subfile_path = dst_directory + src_filename + "_part_1.jsonl"
        current_subfile = open(subfile_path, "w")
        file_size = 0
        request_count = 1
        total_request_count = 0
        for line in main_file:
            line_size = utf8len(line)
            total_request_count += 1

            if (
                request_count + 1 > request_limit
            ) or file_size + line_size > size_limit:
                if request_count == 1:
                    print(
                        f"Limits are too strict! Aborting at file {subfiles_count}..."
                    )
                    break
                current_subfile.close()
                subfiles_count += 1
                subfile_path = (
                    dst_directory + src_filename + f"_part_{subfiles_count}.jsonl"
                )
                current_subfile = open(subfile_path, "w")
                file_size = line_size
                request_count = 1
            else:
                file_size += line_size
                request_count += 1

            print(
                f"Writing request {total_request_count} at line {request_count} in file {subfiles_count}               ",
                end="\r",
            )
            current_subfile.write(line)
    winsound.Beep(1000, 300)
    winsound.Beep(2000, 300)
    winsound.Beep(2500, 300)


def prepare_batch_jobs(dst_filepath, src_directory):
    # given a src_directory containing jsonl-files, create a file at the dst_filepath to track them
    # and upload each file, saving the returned file IDs in the tracking file
    directory_content = os.listdir(src_directory)
    directory_jsonl_files = filter(lambda x: x.endswith(".jsonl"), directory_content)

    # if the tracking file does not exist, write it line by line
    if not os.path.isfile(dst_filepath):
        with open(dst_filepath, "w") as tracking_file:
            tracking_file.write(
                "Batch ID,Filename,Input File ID,Status,Started,Downloaded\n"
            )
            for jsonl in directory_jsonl_files:
                tracking_file.write(f"{None},{jsonl},{None},{None},{False},{False}\n")

    # upload the files that are missing file IDs in the tracking file and save those IDs to the file
    tracked_jobs = dict_list_from_csv(dst_filepath)
    for job in tracked_jobs:
        if job["Input File ID"] == "None":
            jsonl = job["Filename"]
            print(f"Uploading {jsonl} and tracking ID...", end="\r")
            path = src_directory + jsonl
            uploaded_file = client.files.create(file=open(path, "rb"), purpose="batch")
            job.update({"Input File ID": uploaded_file.id})
            # NOTE we update the tracking file after every successful upload in case something goes wrong with a later upload
            # REVIEW if we can find out which exceptions happen when a file creation goes wrong, this could be done only in a try-except and at the end
            dict_list_to_csv(dst_filepath, tracked_jobs)
            print(f"Uploaded {jsonl} successfully. Tracking file updated.")
    winsound.Beep(1000, 300)
    winsound.Beep(2000, 300)
    winsound.Beep(2500, 300)


def run_batch_jobs(src_filepath, dst_directory, job_budget=10, update_frequency=600):
    # given a tracking file containing file IDs, create a batch job for each file while trying not to hit the rate limit
    # and download results of completed jobs to the dst_directory.
    # keeps the number of active jobs to at most job_budget
    # and checks for status updates every update_frequency seconds after starting as many jobs as permitted.
    try:
        os.makedirs(dst_directory)
    except FileExistsError:
        print("Output directory already exists. Make sure you know what you're doing.")
    tracked_jobs = dict_list_from_csv(src_filepath)
    # looping for as long as jobs with undownloaded results exist
    while len(list(filter(lambda job: job["Downloaded"] == "False", tracked_jobs))) > 0:
        # Spawning mode
        pending_jobs = list(filter(lambda job: job["Started"] == "False", tracked_jobs))
        if job_budget > 0 and len(pending_jobs) > 0:
            print("Jobs pending and budget available. Switch to spawning.")
        for tracked_job in pending_jobs:
            if job_budget < 1:
                print("Budget depleted. Switch to tracking.")
                break
            # try to start a job
            print(f"Creating batch job for {tracked_job["Filename"]}")
            batch_job = client.batches.create(
                input_file_id=tracked_job["Input File ID"],
                endpoint="/v1/chat/completions",
                completion_window="24h",
            )
            status = batch_job.status
            while status == "validating":
                ticker_string_sleep("Validating job. Checking status in ", 20, ".")
                status = client.batches.retrieve(batch_job.id).status
            # job is done validating. Check if we succeeded in creating it
            tracked_job.update({"Batch ID": batch_job.id, "Status": status})
            if status == "failed":
                print(
                    f"Batch creation failed for {tracked_job["Filename"]}. Switch to tracking."
                )
                dict_list_to_csv(src_filepath, tracked_jobs)
                job_budget = 0
                break
            else:
                print(f"Batch successfully created for {tracked_job["Filename"]}!")
                tracked_job.update({"Started": "True"})
                dict_list_to_csv(src_filepath, tracked_jobs)
                job_budget -= 1
        # Tracking mode
        running_jobs = list(
            filter(
                lambda job: job["Downloaded"] == "False"
                and job["Status"] in ["in_progress", "finalizing"],
                tracked_jobs,
            )
        )
        # stay tracking if the budget is less than 1 or if there are no jobs to start *and* at least one job is in progress.
        # NOTE this is "while True" because we negate the loop condition and use that to break the loop later
        while True:
            for running_job in running_jobs:
                batch_job = client.batches.retrieve(running_job["Batch ID"])
                status = batch_job.status
                if status != running_job["Status"]:
                    print(
                        f"{datetime.now()} \tStatus update for {running_job["Filename"]} registered: {running_job["Status"]} -> {status}"
                    )
                    running_job.update({"Status": status})
                    dict_list_to_csv(src_filepath, tracked_jobs)
                if status == "completed":
                    # download the output content of the completed job
                    name, extension = os.path.splitext(running_job["Filename"])
                    path = dst_directory + name + "_output" + extension
                    with open(path, "wb") as output_file:
                        result = client.files.content(batch_job.output_file_id).content
                        output_file.write(result)
                        print(
                            f"Result of batch for {running_job["Filename"]} saved to\n{path}"
                        )
                    running_job.update({"Downloaded": "True"})
                    job_budget += 1
                    dict_list_to_csv(src_filepath, tracked_jobs)
            # reevaluate how many jobs are currently running
            running_jobs = list(
                filter(
                    lambda job: job["Downloaded"] == "False"
                    and job["Status"] in ["in_progress", "finalizing"],
                    tracked_jobs,
                )
            )
            if not (
                (job_budget < 1 or len(pending_jobs) == 0) and len(running_jobs) > 0
            ):
                break
            ticker_string_sleep(
                "Checking for status updates in ", update_frequency, "."
            )
    winsound.Beep(1000, 300)
    winsound.Beep(2000, 300)
    winsound.Beep(2500, 300)


def spawn_batch_jobs(dst_filepath, src_directory, initial_backoff_time=600):
    # given a directory of jsonl-files, create a batch job for each file and write the batch IDs at the given filepath.
    directory_content = os.listdir(src_directory)
    directory_jsonl_files = filter(lambda x: x.endswith(".jsonl"), directory_content)

    # if the tracking file already exists, don't write a new one
    if not os.path.isfile(dst_filepath):
        with open(dst_filepath, "w") as tracking_file:
            tracking_file.write(
                "Batch ID,Filename,Input File ID,Status,Started,Downloaded\n"
            )
            for jsonl in directory_jsonl_files:
                tracking_file.write(f"{None},{jsonl},{None},{None},{False},{False}\n")

    # get the files that exist on the client before we start working
    available_files = client.files.list()
    available_files_names = [item.filename for item in available_files]

    # make a list of the names of the files we should upload and start batches for
    tracking_file = open(dst_filepath, "r")
    dict_reader = csv.DictReader(tracking_file)
    tracked_jobs = list(dict_reader)
    tracking_file.close()
    jobs_to_start = []
    for tracked_job in tracked_jobs:
        if tracked_job["Started"] == "False":
            jobs_to_start.append(tracked_job["Filename"])

    for jsonl in jobs_to_start:
        batch_started = False
        backoff_time = initial_backoff_time

        # upload the file if it's not available already
        if jsonl not in available_files_names:
            print(f"{jsonl} not available. Uploading...")
            path = src_directory + jsonl
            batch_file = client.files.create(file=open(path, "rb"), purpose="batch")
        else:
            index = available_files_names.index(jsonl)
            batch_file = list(available_files)[index]
        while not batch_started:
            winsound.Beep(1000, 500)
            try:
                # start the batch job
                file_id = batch_file.id
                batch_job = client.batches.create(
                    input_file_id=file_id,
                    endpoint="/v1/chat/completions",
                    completion_window="24h",
                )

                # NOTE the batch job might still fail here if it exceeds rate limits, so let's wait and see if it does
                status = batch_job.status
                while status != "in_progress" and status != "failed":
                    for i in range(20):
                        print(
                            f'DO NOT TURN OFF THE PROGRAM. Waiting for confirmation: Status was "{status}" {i} seconds ago.           ',
                            end="\r",
                        )
                        time.sleep(1)
                    status = client.batches.retrieve(batch_job.id).status
                if status == "failed":
                    # Hitting the rate limit is the most likely culprit for a job failing right after validation,
                    # but we have to use a custom exception since RateLimitError only gets raised on APIs other than the Batch API
                    raise ValidationFailedException(
                        f"Validation failed for batch {batch_job.id}"
                    )

                # we managed to create a working batch job! Update the tracking file and get ready for the next one
                batch_started = True
                winsound.Beep(2000, 500)
                print(f"\nCreated job for {jsonl}!")
                for tracked_job in tracked_jobs:
                    # find the tracked job with the right filename. # NOTE we assume there are no duplicate filenames
                    if tracked_job["Filename"] == jsonl:
                        tracked_job.update(
                            {
                                "Batch ID": batch_job.id,
                                "Input File ID": file_id,
                                "Status": batch_job.status,
                                "Started": "True",
                            }
                        )
                        with open(dst_filepath, "w", newline="") as tracking_file:
                            keys = tracked_jobs[0].keys()
                            dict_writer = csv.DictWriter(tracking_file, keys)
                            dict_writer.writeheader()
                            dict_writer.writerows(tracked_jobs)
                        break
            except ValidationFailedException:
                winsound.Beep(500, 500)
                for i in range(backoff_time):
                    print(
                        f"Batch job failed! Waiting {backoff_time-i} seconds before trying to create a batch job for {jsonl}.     ",
                        end="\r",
                    )
                    time.sleep(1)
                backoff_time *= 2
    # celebratory beeps to indicate that we're done
    winsound.Beep(1000, 300)
    winsound.Beep(2000, 300)
    winsound.Beep(2500, 300)


def track_batches(src_filepath, dst_directory):
    # given the path to a file where each line is a batch ID, track their completeness and save the contents of completed batches to the given directory
    # create the directory we need to put our subfiles into
    try:
        os.makedirs(dst_directory)
    except FileExistsError:
        print("Output directory already exists. Make sure you know what you're doing.")
    while True:
        winsound.Beep(1000, 200)
        winsound.Beep(1000, 200)
        tracker = open(src_filepath, "r")
        dict_reader = csv.DictReader(tracker)
        tracked_jobs = list(dict_reader)
        tracker.close()
        downloaded_results = 0
        for tracked_job in tracked_jobs:
            batch_job = client.batches.retrieve(tracked_job["Batch ID"])
            if batch_job.status != tracked_job["Status"]:
                # keep track of status updates for all the batches
                print(
                    f"{datetime.now()} \tBatch for {tracked_job["Filename"]} status updated: {batch_job.status}"
                )
                tracked_job.update({"Status": batch_job.status})

            if batch_job.status == "completed" and tracked_job["Downloaded"] == "False":
                # download completed batches (only once)
                result = client.files.content(batch_job.output_file_id).content
                name, extension = os.path.splitext(tracked_job["Filename"])
                path = dst_directory + name + "_output" + extension
                with open(path, "wb") as output_file:
                    output_file.write(result)
                    print(
                        f"Result of batch for {tracked_job["Filename"]} saved to {path}"
                    )
                    tracked_job.update({"Downloaded": "True"})
                    # NOTE we have to update with the string-version of "True" so it lines up with what we read from the tracking file
            downloaded_results += tracked_job["Downloaded"] == "True"
        # we've checked all the batches. Now we update the tracking file with what we know
        tracker = open(src_filepath, "w", newline="")
        keys = tracked_jobs[0].keys()
        dict_writer = csv.DictWriter(tracker, keys)
        dict_writer.writeheader()
        dict_writer.writerows(tracked_jobs)
        tracker.close()

        # if we've downloaded all the results, break out of the loop, otherwise wait for a while
        if downloaded_results == len(tracked_jobs):
            print("All batches complete and downloaded!")
            break
        else:
            for i in range(300):
                print(
                    f"Checking for updates in {300-i} seconds.     ",
                    end="\r",
                )
                time.sleep(1)
    # celebratory beeps to indicate that we're done
    winsound.Beep(1000, 300)
    winsound.Beep(2000, 300)
    winsound.Beep(2500, 300)


def combine_jsonl(dst_filepath, src_directory):
    # given a directory of jsonl-files, stitch them together into a bigger jsonl-file at the given filepath. Order of lines not guaranteed!
    with open(dst_filepath, "w") as output_file:
        directory_content = os.listdir(src_directory)
        for jsonl in filter(lambda x: x.endswith(".jsonl"), directory_content):
            path = src_directory + jsonl
            with open(path, "r") as output_subfile:
                line_count = 0
                for line in output_subfile:
                    line_count += 1
                    print(
                        f"Writing line {line_count} of {jsonl} to output file.     ",
                        end="\r",
                    )
                    output_file.write(line)
        print("\nAll done and ready for post-processing!")
    winsound.Beep(1000, 300)
    winsound.Beep(2000, 300)
    winsound.Beep(2500, 300)


if __name__ == "__main__":
    # NOTE make sure all specified directories are empty before using them here!
    main_file = "experiments/partScores200ConfScore/partScores200ConfScore.jsonl"
    subfiles_directory = (
        "experiments/partScores200ConfScore/partScores200ConfScoresplit/"
    )
    tracking_file = subfiles_directory + "tracker.csv"
    output_directory = (
        "experiments/partScores200ConfScore/partScores200ConfScoresplitOutput/"
    )
    output_file = (
        "experiments/partScores200ConfScore/partScores200ConfScoreoutput.jsonl"
    )

    with open("secrets.json", "r") as file:
        secrets = json.load(file)
        global client
        client = openai.OpenAI(
            organization=secrets["organization"],
            project=secrets["project"],
            api_key=secrets["api_key"],
        )

    split_jsonl(main_file, subfiles_directory)
    prepare_batch_jobs(tracking_file, subfiles_directory)
    run_batch_jobs(tracking_file, output_directory)
    combine_jsonl(output_file, output_directory)
