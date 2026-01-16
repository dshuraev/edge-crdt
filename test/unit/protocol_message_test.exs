defmodule EdgeCrdtTest.Unit.ProtocolMessageTest do
  use ExUnit.Case, async: true

  alias EdgeCrdt.Protocol.Message
  alias EdgeCrdt.Protocol.Message.{DigestRequest, DigestResponse, Header, SyncRequest, SyncResponse}

  @replica_id "000000000000000a"
  @crdt_id "1111111111111111"

  defp header_for(type) do
    %Header{protocol_version: 1, message_type: type, flags: [], length: 0}
  end

  defp digest do
    %{@crdt_id => {@replica_id, 1}}
  end

  defp bundle do
    %{@crdt_id => [{{@replica_id, 1}, %{@replica_id => 1}}]}
  end

  test "digest request round-trip via payload and message" do
    payload = %DigestRequest{}

    assert {:ok, <<>>} = DigestRequest.encode(payload)
    assert {:ok, ^payload} = DigestRequest.decode(<<>>)

    assert {:ok, message} = Message.encode(header_for(DigestRequest), payload)
    assert {:ok, decoded} = Message.decode(message.encoded)
    assert decoded.payload == payload
    assert decoded.header.message_type == DigestRequest
  end

  test "digest response round-trip via payload and message" do
    payload = %DigestResponse{digest: digest()}

    assert {:ok, encoded} = DigestResponse.encode(payload)
    assert {:ok, decoded_digest} = DigestResponse.decode(encoded)
    assert decoded_digest == payload.digest

    assert {:ok, message} = Message.encode(header_for(DigestResponse), payload)
    assert {:ok, decoded} = Message.decode(message.encoded)
    assert decoded.payload == payload.digest
    assert decoded.header.message_type == DigestResponse
  end

  test "sync request round-trip via payload and message" do
    payload = %SyncRequest{sync_type: :delta, digest: digest(), include_digest?: true}

    assert {:ok, encoded} = SyncRequest.encode(payload)
    assert {:ok, ^payload} = SyncRequest.decode(encoded)

    assert {:ok, message} = Message.encode(header_for(SyncRequest), payload)
    assert {:ok, decoded} = Message.decode(message.encoded)
    assert decoded.payload == payload
    assert decoded.header.message_type == SyncRequest
  end

  test "sync response round-trip via payload and message" do
    payload = %SyncResponse{bundle: bundle(), digest: digest()}

    assert {:ok, encoded} = SyncResponse.encode(payload)
    assert {:ok, ^payload} = SyncResponse.decode(encoded)

    assert {:ok, message} = Message.encode(header_for(SyncResponse), payload)
    assert {:ok, decoded} = Message.decode(message.encoded)
    assert decoded.payload == payload
    assert decoded.header.message_type == SyncResponse
  end
end
