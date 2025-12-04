defmodule EdgeCrdtTest do
  use ExUnit.Case
  doctest EdgeCrdt

  test "greets the world" do
    assert EdgeCrdt.hello() == :world
  end
end
