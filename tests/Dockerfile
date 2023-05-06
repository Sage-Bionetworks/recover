FROM amazon/aws-glue-libs:glue_libs_4.0.0_image_01

RUN pip3 install moto~=4.1 synapseclient~=2.7 pyarrow~=11.0 datacompy~=0.8 pytest-datadir
ENTRYPOINT ["bash", "-l"]
