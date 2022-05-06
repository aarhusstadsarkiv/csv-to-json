import argparse
import json
from os.path import exists
from csv import DictReader, QUOTE_MINIMAL

from typing import List, Dict, Any


# setting csv field size limit to the biggest value allowed
# (which is based on the size of a signed C long, 'cus that's how Python does things apparently)
import csv

C_LONG_MAX = 2147483647
csv.field_size_limit(C_LONG_MAX)


def cirius(folder: str) -> None:
    """
    * Takes a path to a folder containing the .csv files
      generated from specific SQL queries
    * Produces a .json file with all the info neatly organized
    """
    # specifiying the files we need and checking that they exist
    csv_fil = folder + r"\fil.csv"
    csv_sag = folder + r"\sag.csv"
    csv_dokumentcdw = folder + r"\dokumentcdw.csv"
    csv_notat = folder + r"\notat.csv"

    if not (
        exists(csv_fil)
        and exists(csv_sag)
        and exists(csv_dokumentcdw)
        and exists(csv_notat)
    ):
        raise Exception(
            "Required .csv files weren't found in the specified directory"
        )

    # to keep things efficient in terms of performance speed,
    # we're only gonna read thru each file once, and only ever do
    # simple dict lookups rather than searching thru its contents

    # So to facilitate this, we're gonna start by looking thru fil.csv
    # And we're gonna make 3 dicts, one for each kind of thing that a file
    # can be attached to
    dokumentFiles: Dict[str, List[Dict[str, str]]] = dict()
    cdwFiles: Dict[str, List[Dict[str, str]]] = dict()
    notatFiles: Dict[str, List[Dict[str, str]]] = dict()
    # ... and also we're gonna make a mapping between these and the
    # names used in fil.csv to specify which object it belongs to
    dict_mapping = {
        "dokument": dokumentFiles,
        "cdw": cdwFiles,
        "notat": notatFiles,
    }

    # then we're gonna read thru fil.csv using DictReader,
    # saving us the trouble of making each file into a dict ourselves
    with open(csv_fil, newline="", encoding="utf-8") as csv:
        reader = DictReader(csv, delimiter=";", quotechar='"')
        for fil in reader:
            # for each file, we're gonna check what kind of object it belongs to
            # and put it inside the appropriate dict, using the object's id as the key
            # so we can easily find it later
            appropriate_dict = dict_mapping[fil["notes_template_name"]]
            if fil["notes_template_id"] not in appropriate_dict:
                appropriate_dict[fil["notes_template_id"]] = []
            # we're using a list to keep the files, since a single
            # object can have multiple files attached to it
            appropriate_dict[fil["notes_template_id"]].append(fil)

    # now that we've got that handled, we can move on to the
    # outermost "layer" of our .json - sag.csv
    # Since our "sager" need to be in a list, we're gonna make a dict
    # to map our key value, "SagsNr", to the index of that "sag" in our list
    sager: List[Dict[str, Any]] = []
    index_mapping_sager: Dict[str, int] = dict()

    with open(csv_sag, newline="", encoding="utf-8") as csv:
        reader = DictReader(csv, delimiter=";", quotechar='"')
        for sag in reader:
            sager.append(sag)
            index_mapping_sager[sag["SagsNr"]] = len(sager) - 1

    # we need ANOTHER list and dict,
    # in order to keep track of "dokumenter"
    # while also properly handling "cdwer"
    dokumenter: List[Dict[str, Any]] = []
    index_mapping_dokumenter: Dict[str, int] = dict()

    # and also, for convenience, I'm gonna define specific helper methods here,
    # cutting down on the amount of parameters I need to
    # provide when wanting to append objects later.
    def append_to_sag(sagsNr: str, obj_type: str, obj: Dict[str, Any]):
        append_to_obj(sager, index_mapping_sager, sagsNr, obj_type, obj)

    def append_to_dokument(dokId: str, obj: Dict[str, Any]):
        append_to_obj(
            dokumenter, index_mapping_dokumenter, dokId, "cdwListe", obj
        )

    # now we can start processing "dokumentcdw.csv"
    with open(csv_dokumentcdw, newline="", encoding="utf-8") as csv:
        reader = DictReader(
            csv, delimiter=";", quotechar='"', quoting=QUOTE_MINIMAL
        )
        for dokumentcdw in reader:
            # split "dokumentcdw" into "dokument" and "cdw"
            dokument: Dict[str, Any] = dict()
            cdw: Dict[str, Any] = dict()
            # all the info cdw has
            # the rest is for dokument
            cdw_keys = [
                "cdw_id",
                "cdwDocumentUniqueID",
                "cdwCreatedDate",
                "From1",
                "PostedDate",
                "SendTo",
                "CopyTo",
                "BlindCopyTo",
                "Subject",
                "cdwBody",
            ]
            # now we can iterate thru all the values in our dokumentcdw dict
            # and copy them into the appropriate dict
            for key, value in dokumentcdw.items():
                if value is None:
                    value = ""
                if key in cdw_keys:
                    cdw[key] = value
                else:
                    dokument[key] = value

            # check if dokument is already in our dict
            # if not, we'll process it
            # otherwise we've already seen this dokument before
            # so this information is already known to us
            if dokument["dokument_id"] not in dokumenter:
                # add filListe to dokument,
                # if it has any
                if dokument["dokument_id"] in dokumentFiles:
                    dokument["filListe"] = dokumentFiles[
                        dokument["dokument_id"]
                    ]

                # append dokument to "dokumenter"
                # and also, update "index_mapping_dokumenter"
                # similarly to what we did with "sager" earlier
                dokumenter.append(dokument)
                index_mapping_dokumenter[dokument["dokument_id"]] = (
                    len(dokumenter) - 1
                )

            # now start processing the cdw portion!
            # check if there IS any cdw info at all,
            # 'cus a dokument might not have any
            if cdw["cdw_id"] != "":
                # add filListe to cdw
                if cdw["cdw_id"] in cdwFiles:
                    cdw["filListe"] = cdwFiles[cdw["cdw_id"]]

                # append cdw to the corresponding dokument,
                append_to_dokument(dokumentcdw["dokument_id"], cdw)

    # add all dokumenter to their respecticve sager
    for dokument in dokumenter:
        sagsnr = dokument["SagsNr"]
        append_to_sag(sagsnr, "dokumentListe", dokument)

    with open(csv_notat, newline="", encoding="utf-8") as csv:
        reader = DictReader(csv, delimiter=";", quotechar='"')
        for notat in reader:
            # before we add the "notat" to our "sag",
            # we're gonna check if the "notat" has any files
            # If it does, we add the list to this "notat"
            if notat["notat_id"] in notatFiles:
                notat["filListe"] = notatFiles[notat["notat_id"]]

            # and we're also gonna check if our "sag" has a list of "notater"
            # we can append to; if not, we create one
            sagsnr = notat["SagsNr"]
            append_to_sag(sagsnr, "notatListe", notat)

    # finally, we can encode our list of "sager" as .json,
    # and write it to file
    json_sager = json.JSONEncoder(indent=4).encode(sager)
    with open(folder + r"\cirius.json", "w") as f:
        f.write(json_sager)


# generic helper method, to cut down on repeated code
# handles the appending of a "child object" to their corresponding "parent object"
# (like for example a "dokument" to a "sag")
def append_to_obj(
    parent_objects: List[Dict[str, Any]],
    parent_index_map: Dict[str, int],
    index_key: str,
    child_obj_type: str,
    child_obj: Dict[str, Any],
) -> None:
    # find the object that this object needs to be appended to
    # by checking our index_map with the key,
    # and then using the result to index into the object list
    if index_key not in parent_index_map:
        print(
            f"ERROR: following object was not found using index {index_key}: \n{child_obj}"
        )
        return

    corresponding_obj = parent_objects[parent_index_map[index_key]]
    # check if this object has a list to append our object to.
    # if not, we create one
    if child_obj_type not in corresponding_obj:
        corresponding_obj[child_obj_type] = []
    # finally, we add the object to the list
    corresponding_obj[child_obj_type].append(child_obj)


# Set up argparse stuff

parser = argparse.ArgumentParser()
parser.add_argument(
    "folder",
    help="the path to the directory where the " + ".csv files are located",
    type=str,
)
args: argparse.Namespace = parser.parse_args()
cirius(args.folder)
