#!/usr/bin/env python
"""
Download from W&B the raw dataset and apply some basic data cleaning, exporting the result to a new artifact
"""
import argparse
import logging

import pandas as pd
import wandb


logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):

    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    # Download input artifact. This will also log that this script is using this
    # particular version of the artifact
    artifact_local_path = run.use_artifact(args.input_artifact).file()

    # Load the raw dataset
    df = pd.read_csv(artifact_local_path)

    # filter outliers by price
    min_price = args.min_price
    max_price = args.max_price
    df_clean = df[(df['price'] >= min_price) & (df['price'] <= max_price)]

    # save clean df
    df_clean.to_csv(args.output_artifact, index=False)

    # log clean artifact to W&B
    artifact_clean = wandb.Artifact(
        name=args.output_artifact,
        type=args.output_type,
        description=args.output_description,
    )

    artifact_clean.add_file("clean_sample.csv")
    run.log_artifact(artifact_clean)

    run.finish()

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Basic data cleaning")


    parser.add_argument(
        "--input_artifact", 
        type=str,
        help="W&B artifact name for raw dataset",
        required=True
    )

    parser.add_argument(
        "--output_artifact", 
        type=str,
        help="name for clean dataset artifact",
        required=True
    )

    parser.add_argument(
        "--output_type", 
        type=str,
        help="type of output artifact",
        required=True
    )

    parser.add_argument(
        "--output_description", 
        type=str,
        help="decsription of clean dataset artifact",
        required=True
    )

    parser.add_argument(
        "--min_price", 
        type=float,
        help="minimum price to filter outliers",
        required=True
    )

    parser.add_argument(
        "--max_price", 
        type=float,
        help="maximum price to filter outliers",
        required=True
    )


    args = parser.parse_args()

    go(args)
