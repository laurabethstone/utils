import os
import shutil
import zipfile

def zip_and_remove_folder(folder_to_zip, zip_file_name):
    """Zips a folder and then removes the original folder."""
    with zipfile.ZipFile(zip_file_name, 'w', compression=zipfile.ZIP_DEFLATED) as zipf:
        for root, dirs, files in os.walk(folder_to_zip):
            for file in files:
                # zipf.write(os.path.join(root, file))
                zipf.write(os.path.join(root, file),
                           arcname=os.path.join(root.replace(folder_to_zip, '', 1), file))
    shutil.rmtree(folder_to_zip)

def unzip_to_folder(zip_file,output_folder):
    os.mkdir(output_folder)
    with zipfile.ZipFile(zip_file, 'r') as zip_ref:
        zip_ref.extractall(output_folder)


if __name__ == "__main__":
    folder_to_zip = "/Users/J7UHZWT/Documents/GitHub/test-dashboards/dashboards"
    zip_filename = "dashboards.zip"
    output_folder = "/Users/J7UHZWT/Documents/GitHub/test-dashboards/dashboards"
    zip_and_remove_folder(folder_to_zip, zip_filename)
    # unzip_to_folder(zip_filename,output_folder)

