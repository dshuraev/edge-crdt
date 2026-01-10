defmodule EdgeCrdtTest.Unit.ContextTest do
  use ExUnit.Case, async: true

  doctest EdgeCrdt.Replica.Context
  alias EdgeCrdt.Replica.Context

  @replica_a "000000000000000a"
  @replica_b "000000000000000b"

  describe "new/0" do
    test "creates an empty context struct" do
      assert Context.new() |> Context.empty?()
    end
  end

  describe "new/1" do
    test "creates empty context from empty enum" do
      assert Context.new([]) |> Context.empty?()
    end

    test "creates context from enumerable of dots" do
      assert Context.new([{@replica_a, 1}, {@replica_b, 42}])
             |> Context.contains?({@replica_b, 42})
    end

    test "deduplicates dots for the same replica" do
      ctx = Context.new([{@replica_a, 1}, {@replica_a, 1}, {@replica_a, 2}])

      assert Context.contains?(ctx, {@replica_a, 1})
      assert Context.contains?(ctx, {@replica_a, 2})
      assert Context.new([{@replica_a, 1}, {@replica_a, 2}]) |> Context.eq?(ctx)
    end
  end

  describe "eq/2" do
    test "returns true on empty contexts" do
      assert Context.eq?(Context.new(), Context.new())
    end

    test "returns true when two contexts are equal" do
      dots = [{@replica_a, 1}, {@replica_b, 42}]
      context1 = Context.new(dots)
      context2 = Context.new(dots)
      assert Context.eq?(context1, context2)
    end

    test "returns false when two context differ" do
      dots = [{@replica_a, 1}]
      context1 = Context.new(dots)
      context2 = Context.new([{@replica_b, 42}] ++ dots)
      refute Context.eq?(context1, context2)
    end

    test "ignores ordering of dots" do
      context1 = Context.new([{@replica_a, 1}, {@replica_b, 2}, {@replica_a, 3}])
      context2 = Context.new([{@replica_a, 3}, {@replica_b, 2}, {@replica_a, 1}])

      assert Context.eq?(context1, context2)
    end
  end

  describe "lt/2" do
    test "returns false on empty contexts" do
      refute Context.lt?(Context.new(), Context.new())
    end

    test "returns false if two contexts are equal" do
      dots = [{@replica_a, 1}, {@replica_b, 42}]
      context1 = Context.new(dots)
      context2 = Context.new(dots)
      refute Context.lt?(context1, context2)
    end

    test "returns true if the first context is a strict subset of the second" do
      dots = [{@replica_a, 1}]
      context1 = Context.new(dots)
      context2 = Context.new([{@replica_a, 2} | dots])
      assert Context.lt?(context1, context2)
    end

    test "returns false if the first context is greater than the second" do
      dots = [{@replica_a, 1}]
      context1 = Context.new(dots)
      context2 = Context.new([{@replica_a, 2} | dots])
      refute Context.lt?(context2, context1)
    end

    test "considers dots grouped by replica" do
      context1 = Context.new([{@replica_a, 1}, {@replica_b, 1}])
      context2 = Context.new([{@replica_a, 1}, {@replica_a, 2}, {@replica_b, 1}])

      assert Context.lt?(context1, context2)
    end

    test "treats empty as smaller than non-empty" do
      refute Context.lt?(Context.new(), Context.new())
      assert Context.lt?(Context.new(), Context.new([{@replica_a, 1}]))
    end

    test "returns false when a replica is missing progress" do
      left = Context.new([{@replica_a, 1}])
      right = Context.new([{@replica_b, 1}])

      refute Context.lt?(left, right)
      refute Context.lt?(right, left)
    end
  end

  describe "since/2" do
    test "when called on empty context always returns empty context" do
      assert Context.since(Context.new(), Context.new()) |> Context.empty?()
      assert Context.since(Context.new(), Context.new([{@replica_a, 1}])) |> Context.empty?()
    end

    test "when called with empty since-context returns original context" do
      dots = [{@replica_a, 1}]
      context1 = Context.new(dots)
      assert Context.since(context1, Context.new()) |> Context.eq?(context1)
    end

    test "returns dots not present in since-context" do
      base = [{@replica_a, 1}]
      ctx_dot = [{@replica_b, 2}]
      since_dot = [{@replica_b, 3}]
      ctx = Context.new(base ++ ctx_dot)
      since = Context.new(base ++ since_dot)
      assert Context.since(ctx, since) |> Context.eq?(Context.new(ctx_dot))
    end

    test "drops empty replica entries from the delta" do
      ctx = Context.new([{@replica_a, 1}, {@replica_b, 2}])
      since = Context.new([{@replica_a, 1}])

      assert Context.since(ctx, since) |> Context.eq?(Context.new([{@replica_b, 2}]))
    end

    test "ignores dots that are only present in since-context" do
      ctx = Context.new([{@replica_a, 1}])
      since = Context.new([{@replica_a, 1}, {@replica_b, 2}])

      assert Context.since(ctx, since) |> Context.empty?()
    end

    test "returns empty when since is ahead on the same replica" do
      ctx = Context.new([{@replica_a, 1}])
      since = Context.new([{@replica_a, 1}, {@replica_a, 2}])

      assert Context.since(ctx, since) |> Context.empty?()
    end
  end

  describe "join/2" do
    test "unions dots from both contexts" do
      ctx_a = Context.new([{@replica_a, 1}])
      ctx_b = Context.new([{@replica_a, 2}, {@replica_b, 1}])

      assert Context.join(ctx_a, ctx_b)
             |> Context.eq?(Context.new([{@replica_a, 1}, {@replica_a, 2}, {@replica_b, 1}]))
    end

    test "is commutative and idempotent" do
      ctx_a = Context.new([{@replica_a, 1}])
      ctx_b = Context.new([{@replica_a, 2}, {@replica_b, 1}])

      assert Context.join(ctx_a, ctx_b) |> Context.eq?(Context.join(ctx_b, ctx_a))
      assert Context.join(ctx_a, ctx_a) |> Context.eq?(ctx_a)
    end
  end
end
