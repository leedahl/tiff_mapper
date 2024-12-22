from io import BytesIO
from math import ceil
from typing import Optional

import ray
import click
from geoparquet.factory import GeoParquet
from geoparquet.column_definition import ColumnDefinition
from parquet.thrift_types import UUIDType, FieldRepetitionType, DecimalType, IntType, StringType
from os import path, listdir
from uuid import uuid4
from tifffile import TiffFile
from struct import pack


@click.group()
def cli():
    pass

@click.command()
@click.argument('input-path', type=click.Path(exists=True))
@click.argument('output-path', type=click.Path())
@click.option('-ra', '--ray-address', required=False, default=None, type=str)
def mapper(input_path: str, output_path: str, ray_address: Optional[str]) -> None:
    if not path.isdir(input_path):
        raise RuntimeError('The input path needs to be a directory that contains TIFF files to map.')

    if not ray.is_initialized():
        if ray_address is not None:
            ray.init(_node_ip_address=ray_address)

        else:
            ray.init()

    base_path = path.split(output_path)[0]
    id_path = f'{base_path}/id.parquet'
    filename_path = f'{base_path}/filename.parquet'
    gsd_path = f'{base_path}/gsd.parquet'
    class_path = f'{base_path}/class.parquet'
    width_path = f'{base_path}/width.parquet'
    height_path = f'{base_path}/height.parquet'
    polygon_path = f'{base_path}/polygon.parquet'
    with GeoParquet(output_path, mode='x') as file:
        click.echo('Initialized the database')
        file.schema.extend([
            ColumnDefinition(
                'id', cardinality=FieldRepetitionType.REQUIRED, logical_type=UUIDType(), file_url=id_path,
                add_file_metadata=True
            ),
            ColumnDefinition(
                'filename', cardinality=FieldRepetitionType.OPTIONAL, logical_type=StringType(),
                file_url=filename_path, add_file_metadata=True
            ),
            ColumnDefinition(
                'class', cardinality=FieldRepetitionType.REQUIRED, logical_type=StringType(),
                file_url=class_path, add_file_metadata=True
            ),
            ColumnDefinition(
                'gsd', cardinality=FieldRepetitionType.REQUIRED,
                logical_type=DecimalType(11, 12), file_url=gsd_path, add_file_metadata=True
            ),
            ColumnDefinition(
                'width', cardinality=FieldRepetitionType.REQUIRED,
                logical_type=IntType(16, False), file_url=width_path, add_file_metadata=True
            ),
            ColumnDefinition(
                'height', cardinality=FieldRepetitionType.REQUIRED,
                logical_type=IntType(16, False), file_url=height_path, add_file_metadata=True
            ),
            ColumnDefinition(
                'polygon', cardinality=FieldRepetitionType.REQUIRED,
                geo_metadata={'version': '1.1.0', 'column': {'encoding': 'WKB', 'geometry_types': ['Polygon']}},
                file_url=polygon_path, add_file_metadata=True
            )
        ])

        min_x = None
        max_x = None
        min_y = None
        max_y = None
        average_gsd = 0
        total_files = 0
        set_min_max = False
        for file_name in listdir(input_path):
            if file_name.endswith('.tif'):
                with TiffFile(f'{input_path}/{file_name}') as tiff:
                    total_files += 1
                    uuid_id = uuid4().bytes
                    image_width = tiff.pages[0].imagewidth
                    image_height = tiff.pages[0].imagelength
                    gsd = max(tiff.pages[0].geotiff_tags.get('ModelPixelScale'))

                    upper_left_x = tiff.pages[0].geotiff_tags.get('ModelTiepoint')[3]
                    upper_left_y = tiff.pages[0].geotiff_tags.get('ModelTiepoint')[4]
                    lower_left_x = upper_left_x
                    lower_left_y = upper_left_y - image_height * gsd
                    lower_right_x = upper_left_x + image_width * gsd
                    lower_right_y = lower_left_y
                    upper_right_x = lower_right_x
                    upper_right_y = upper_left_y

                    if set_min_max is False:
                        min_x = upper_left_x
                        max_x = upper_left_x
                        min_y = upper_left_y
                        max_y = lower_left_y
                        set_min_max = True
                        average_gsd = gsd

                    else:
                        min_x = min(min_x, upper_left_x, lower_left_x, lower_right_x, upper_right_x)
                        max_x = max(max_x, upper_right_x, lower_right_x, lower_left_x, upper_left_x)
                        min_y = min(min_y, upper_left_y, lower_left_y, lower_right_y, upper_right_y)
                        max_y = max(max_y, upper_right_y, lower_right_y, lower_left_y, upper_left_y)
                        average_gsd += gsd

                    wkb = _create_wkb_bounding_box(
                        lower_left_x, lower_left_y, lower_right_x, lower_right_y,
                        upper_left_x, upper_left_y, upper_right_x, upper_right_y
                    )
                    file.insert_records(
                        ['id', 'filename', 'class', 'gsd', 'width', 'height', 'polygon'],
                        [[uuid_id, file_name, 'image_tile', gsd, image_width, image_height, wkb]]
                    )

        wkb = _create_wkb_bounding_box(min_x, min_y, max_x, min_y, min_x, max_y, max_x, max_y)
        uuid_id = uuid4().bytes
        average_gsd /= total_files
        file_name = path.split(output_path)[1].replace('.parquet', '.zarr')
        image_width = ceil((max_x - min_x) / average_gsd)
        image_height = ceil((max_y - min_y) / average_gsd)
        file.insert_records(
            ['id', 'filename', 'class', 'gsd', 'width', 'height', 'polygon'],
            [[uuid_id, file_name, 'image', average_gsd, image_width, image_height, wkb]]
        )
        start_x = min_x
        start_y = max_y - 256 * average_gsd
        width = ceil(image_width / 256) * 256 + 1
        height = image_height - ceil(image_height / 256) * 256 - 1
        for x in range(0, width, 256):
            for y in range(image_height, height, -256):
                lower_left_x = start_x + x * average_gsd
                lower_left_y = start_y - y * average_gsd
                lower_right_x = lower_left_x + average_gsd * 256
                lower_right_y = lower_left_y
                upper_left_x = lower_left_x
                upper_left_y = lower_left_y + average_gsd * 256
                upper_right_x = lower_right_x
                upper_right_y = upper_left_y

                wkb = _create_wkb_bounding_box(
                    lower_left_x, lower_left_y, lower_right_x, lower_right_y, upper_left_x, upper_left_y,
                    upper_right_x, upper_right_y
                )
                uuid_id = uuid4().bytes
                file.insert_records(
                    ['id', 'filename', 'class', 'gsd', 'width', 'height', 'polygon'],
                    [[uuid_id, None, 'block_tile', average_gsd, 256, 256, wkb]]
                )

def _create_wkb_bounding_box(
        lower_left_x: float, lower_left_y: float, lower_right_x: float, lower_right_y: float,
        upper_left_x: float, upper_left_y: float, upper_right_x: float, upper_right_y: float
) -> bytes:
    """
    Construct a WKB bounding box from the given coordinates. The coordinates are expected to be in geographic
    coordinates.

    :param lower_left_x: The lower left longitude of the bounding box.
    :param lower_left_y: The lower left latitude of the bounding box.
    :param lower_right_x: The lower right longitude of the bounding box.
    :param lower_right_y: The lower right latitude of the bounding box.
    :param upper_left_x: The upper left longitude of the bounding box.
    :param upper_left_y: The upper left latitude of the bounding box.
    :param upper_right_x: The upper right longitude of the bounding box.
    :param upper_right_y: The upper right latitude of the bounding box.
    :return: A byte representation of the WKB bounding box.
    """
    wkb = BytesIO()
    wkb.write(b'\x01')
    wkb.write(int(3).to_bytes(4, byteorder='little', signed=False))
    wkb.write(int(1).to_bytes(4, byteorder='little', signed=False))
    wkb.write(int(5).to_bytes(4, byteorder='little', signed=False))
    wkb.write(
        pack(
            f'<10d',
            upper_left_x, upper_left_y, lower_left_x, lower_left_y, lower_right_x, lower_right_y,
            upper_right_x, upper_right_y, upper_left_x, upper_left_y
        )
    )

    return wkb.getvalue()


cli.add_command(mapper)
