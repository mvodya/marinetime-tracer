from pathlib import Path
import h5py


def open_dataset(path: str | Path, mode: str = "r") -> h5py.File:
    path = Path(path)
    return h5py.File(path, mode)

def print_dataset_structure(ds: h5py.File):
    print("HDF5 Dataset Structure:\n\n")

    for name, obj in ds.items():
        print(f"{name}:")
        if isinstance(obj, h5py.Dataset) and obj.dtype.fields:
            for field_name, (field_dtype, offset, *_) in obj.dtype.fields.items():
                print(f"  - {field_name}: {field_dtype}")
            if len(obj) >= 1:
                print(f"  Example: {obj[1]}")
        if isinstance(obj, h5py.Group):
            if name == "positions" and "tracks" in obj:
                print(f"  /YYYY\n    /MM\n      /DD:")
                for k1, item in obj.items():
                    if k1 == "tracks":
                        continue
                    for _, item in item.items():
                        for _, item in item.items():
                            for field_name, (field_dtype, offset, *_) in item.dtype.fields.items():
                                print(f"        - {field_name}: {field_dtype}")
                            print(f"        Example: {item[0]}")
                            break
                        break
                    break

                print(f"  /tracks\n    /AAAA-BBBB\n      /CCCC-DDDD:")
                for _, item in obj["tracks"].items():
                    for _, item in item.items():
                        for field_name, (field_dtype, offset, *_) in item.dtype.fields.items():
                            print(f"        - {field_name}: {field_dtype}")
                        print(f"        Example: {item[0]}")
                        break
                    break
            else:
                print(f"  /YYYY\n    /MM\n      /DD:")
                for _, item in obj.items():
                    for _, item in item.items():
                        for _, item in item.items():
                            for field_name, (field_dtype, offset, *_) in item.dtype.fields.items():
                                print(f"        - {field_name}: {field_dtype}")
                            print(f"        Example: {item[0]}")
                            break
                        break
                    break
        print("\n")
