#include "postgres_binary_copy.hpp"
#include "postgres_binary_writer.hpp"
#include "duckdb/common/serializer/buffered_file_writer.hpp"
#include "duckdb/common/file_system.hpp"

namespace duckdb {

PostgresBinaryCopyFunction::PostgresBinaryCopyFunction() : CopyFunction("postgres_binary") {
	copy_to_bind = PostgresBinaryWriteBind;
	copy_to_initialize_global = PostgresBinaryWriteInitializeGlobal;
	copy_to_initialize_local = PostgresBinaryWriteInitializeLocal;
	copy_to_sink = PostgresBinaryWriteSink;
	copy_to_combine = PostgresBinaryWriteCombine;
	copy_to_finalize = PostgresBinaryWriteFinalize;
}

struct PostgresBinaryCopyGlobalState : public GlobalFunctionData {
	explicit PostgresBinaryCopyGlobalState(ClientContext &context) {
		copy_state.Initialize(context);
	}

	void Flush(PostgresBinaryWriter &writer) {
		file_writer->WriteData(writer.stream.GetData(), writer.stream.GetPosition());
	}

	void WriteHeader() {
		PostgresBinaryWriter writer(copy_state);
		writer.WriteHeader();
		Flush(writer);
	}

	void WriteChunk(DataChunk &chunk) {
		chunk.Flatten();
		PostgresBinaryWriter writer(copy_state);
		for (idx_t r = 0; r < chunk.size(); r++) {
			writer.BeginRow(chunk.ColumnCount());
			for (idx_t c = 0; c < chunk.ColumnCount(); c++) {
				auto &col = chunk.data[c];
				writer.WriteValue(col, r);
			}
			writer.FinishRow();
		}
		Flush(writer);
	}

	void Flush() {
		// write the footer
		PostgresBinaryWriter writer(copy_state);
		writer.WriteFooter();
		Flush(writer);
		// flush and close the file
		file_writer->Flush();
		file_writer.reset();
	}

public:
	unique_ptr<BufferedFileWriter> file_writer;
	PostgresCopyState copy_state;
};

struct PostgresBinaryWriteBindData : public TableFunctionData {};

unique_ptr<FunctionData> PostgresBinaryCopyFunction::PostgresBinaryWriteBind(ClientContext &context,
                                                                             CopyFunctionBindInput &input,
                                                                             const vector<string> &names,
                                                                             const vector<LogicalType> &sql_types) {
	return make_uniq<PostgresBinaryWriteBindData>();
}

unique_ptr<GlobalFunctionData>
PostgresBinaryCopyFunction::PostgresBinaryWriteInitializeGlobal(ClientContext &context, FunctionData &bind_data,
                                                                const string &file_path) {
	auto result = make_uniq<PostgresBinaryCopyGlobalState>(context);
	auto &fs = FileSystem::GetFileSystem(context);
	result->file_writer = make_uniq<BufferedFileWriter>(fs, file_path);
	// write the header
	result->WriteHeader();
	return std::move(result);
}

unique_ptr<LocalFunctionData>
PostgresBinaryCopyFunction::PostgresBinaryWriteInitializeLocal(ExecutionContext &context, FunctionData &bind_data_p) {
	return make_uniq<LocalFunctionData>();
}

void PostgresBinaryCopyFunction::PostgresBinaryWriteSink(ExecutionContext &context, FunctionData &bind_data_p,
                                                         GlobalFunctionData &gstate_p, LocalFunctionData &lstate,
                                                         DataChunk &input) {
	auto &gstate = gstate_p.Cast<PostgresBinaryCopyGlobalState>();
	gstate.WriteChunk(input);
}

void PostgresBinaryCopyFunction::PostgresBinaryWriteCombine(ExecutionContext &context, FunctionData &bind_data,
                                                            GlobalFunctionData &gstate, LocalFunctionData &lstate) {
}

void PostgresBinaryCopyFunction::PostgresBinaryWriteFinalize(ClientContext &context, FunctionData &bind_data,
                                                             GlobalFunctionData &gstate_p) {
	auto &gstate = gstate_p.Cast<PostgresBinaryCopyGlobalState>();
	// write the footer and close the file
	gstate.Flush();
}

} // namespace duckdb