#include "postgres_connection.hpp"
#include "postgres_conversion.hpp"

namespace duckdb {



void PostgresConnection::CopyChunk(DataChunk &chunk) {
	chunk.Flatten();

	PostgresBinaryWriter writer;
	for (idx_t r = 0; r < chunk.size(); r++) {
		writer.BeginRow(chunk.ColumnCount());
		for (idx_t c = 0; c < chunk.ColumnCount(); c++) {
			auto &col = chunk.data[c];
			switch(col.GetType().id()) {
			case LogicalTypeId::BIGINT: {
				auto data = FlatVector::GetData<int64_t>(col)[r];
				writer.WriteInteger<int64_t>(data);
				break;
			}
			default:
				throw InternalException("Unsupported type for Postgres insert");
			}
		}
	}
	CopyData(writer.serializer.blob.data.get(), writer.serializer.blob.size);
}


}
