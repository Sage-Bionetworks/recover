FROM amazon/aws-glue-libs:glue_libs_4.0.0_image_01

RUN pip3 install synapseclient~=2.7 pyarrow~=11.0 pytest-datadir
ENTRYPOINT ["bash", "-l"]
