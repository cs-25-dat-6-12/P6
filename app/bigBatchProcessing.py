import time
import openai
import json
import os
import winsound
import csv
from datetime import datetime

# process batch files that exceed the request or file size limits imposed by OpenAI
# that's files that exceed 100MB or 50.000 requests.


def utf8len(string):
    # return the number of bytes in a string encoded with utf-8
    return len(string.encode("utf-8"))


def split_jsonl(src_filepath, dst_directory, size_limit_MB=100, request_limit=50000):
    # given the path of a jsonl-file, split it according to the specified size limits and create a number of smaller jsonl-files in the given directory
    size_limit = size_limit_MB * 1000000  # convert MB to Bytes

    with open(src_filepath, "r") as main_file:
        # create the directory we need to put our subfiles into
        try:
            os.makedirs(dst_directory)
        except FileExistsError:
            print(
                "Target directory already exists. Make sure you know what you're doing."
            )

        subfiles_count = 1
        subfile_path = dst_directory + "part_1.jsonl"
        current_subfile = open(subfile_path, "w")
        file_size = 0
        request_count = 1
        for line in main_file:
            line_size = utf8len(line)

            if (
                request_count + 1 > request_limit
            ) or file_size + line_size > size_limit:
                if request_count == 1:
                    print(
                        f"Limits are too strict! Aborting at file {subfiles_count}..."
                    )
                    break
                current_subfile.close()
                subfile_path = dst_directory + f"part_{subfiles_count}.jsonl"
                current_subfile = open(subfile_path, "w")
                subfiles_count += 1
                file_size = line_size
                request_count = 1
            else:
                file_size += line_size
                request_count += 1

            print(
                f"Writing line {request_count} in file {subfiles_count}     ", end="\r"
            )
            current_subfile.write(line)


def spawn_batch_jobs(dst_filepath, src_directory, initial_sleep_time=16):
    # given a directory of jsonl-files, create a batch job for each file and write the batch IDs at the given filepath.

    with open(dst_filepath, "w") as tracking_file:
        tracking_file.write(
            "Batch ID,Filename,Input File ID,Status,Started,Downloaded\n"
        )
        directory_content = os.listdir(src_directory)

        # get the files that exist on the client before we start working
        available_files = client.files.list()
        available_files_names = [item.filename for item in available_files]

        for jsonl in filter(lambda x: x.endswith(".jsonl"), directory_content):
            batch_started = False
            sleep_time = initial_sleep_time
            # upload the file
            # NOTE we check if the file has already been uploaded and only upload if it hasn't!
            if jsonl not in available_files_names:
                path = src_directory + jsonl
                batch_file = client.files.create(file=open(path, "rb"), purpose="batch")
            else:
                batch_file = available_files[available_files_names.index(jsonl)]
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

                    winsound.Beep(2000, 500)
                    print(f"Created job for {jsonl}")
                    tracking_file.write(
                        f"{batch_job.id},{jsonl},{file_id},{batch_job.status},{True},{False}\n"
                    )
                    batch_started = True
                except openai.RateLimitError:
                    winsound.Beep(500, 500)
                    for i in range(sleep_time):
                        print(
                            f"Rate limit hit! Waiting {sleep_time-i} seconds before trying to create a batch job for {jsonl}.     ",
                            end="\r",
                        )
                        time.sleep(1)
                    sleep_time *= 2
        # celebratory beeps to indicate that we're done
        winsound.Beep(1000, 300)
        winsound.Beep(2000, 300)
        winsound.Beep(2500, 300)


def track_batches(src_filepath, dst_directory):
    # given the path to a file where each line is a batch ID, track their completeness and save the contents of completed batches to the given directory
    while True:
        winsound.Beep(1000, 200)
        winsound.Beep(1000, 200)
        tracker = open(src_filepath, "r")
        dict_reader = csv.DictReader(tracker)
        tracked_jobs = list(dict_reader)
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
                path = dst_directory + tracked_job["Filename"]
                with open(path, "w") as output_file:
                    output_file.write(result)
                    print(
                        f"Result of batch for {tracked_job["Filename"]} saved to {path}"
                    )
                    tracked_job.update({"Downloaded": True})
            downloaded_results += tracked_job["Downloaded"] == "True"
        # we've checked all the batches. Now we update the tracking file with what we know
        tracker.close()
        # NOTE we close the file to reopen it in write-mode
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
                        f"Writing line {line_count}/{len(output_subfile)} of {jsonl} to output file.",
                        end="\r",
                    )
                    output_file.write(line)


if __name__ == "__main__":
    # NOTE make sure all specified directories are empty before using them here!
    main_file = "app/partScores400.jsonl"
    subfiles_directory = "app/partScores400split/"
    tracking_file = subfiles_directory + "tracker.csv"
    output_directory = "app/partScores400splitOutput/"
    output_file = "app/partScores400output.jsonl"

    with open("secrets.json", "r") as file:
        secrets = json.load(file)
        global client
        client = openai.OpenAI(
            organization=secrets["organization"],
            project=secrets["project"],
            api_key=secrets["api_key"],
        )

    # split_jsonl(main_file, subfiles_directory)
    # spawn_batch_jobs(tracking_file, subfiles_directory)
    track_batches(tracking_file, output_directory)
    combine_jsonl(output_file, output_directory)
