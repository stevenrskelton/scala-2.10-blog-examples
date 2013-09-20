namespace java ca.stevenskelton.thrift.testservice

struct SampleStruct {
	1: i32 id,
	2: string name
}

exception NotFoundException {
	1: string field
}
exception DisabledException {
	1: string field
}

service TestApi {
    SampleStruct wNoDelay(1: i32 id) throws (1: NotFoundException notFoundException, 2: DisabledException disabledException)
    SampleStruct w100msDelay(1: i32 id) throws (1: NotFoundException notFoundException, 2: DisabledException disabledException)
    SampleStruct w200msDelay(1: i32 id) throws (1: NotFoundException notFoundException, 2: DisabledException disabledException)
    SampleStruct w1sDelay(1: i32 id) throws (1: NotFoundException notFoundException, 2: DisabledException disabledException)
}