defmodule EdgeCrdtTest.Unit.ContextTest do
  use ExUnit.Case, async: true

  doctest EdgeCrdt.Context
  alias EdgeCrdt.Context

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
      assert Context.new([{"a", 1}, {"b", 42}]) |> Context.contains?({"b", 42})
    end

    test "deduplicates dots for the same replica" do
      ctx = Context.new([{"a", 1}, {"a", 1}, {"a", 2}])

      assert Context.contains?(ctx, {"a", 1})
      assert Context.contains?(ctx, {"a", 2})
      assert Context.new([{"a", 1}, {"a", 2}]) |> Context.eq?(ctx)
    end
  end

  describe "eq/2" do
    test "returns true on empty contexts" do
      assert Context.eq?(Context.new(), Context.new())
    end

    test "returns true when two contexts are equal" do
      dots = [{"a", 1}, {"b", 42}]
      context1 = Context.new(dots)
      context2 = Context.new(dots)
      assert Context.eq?(context1, context2)
    end

    test "returns false when two context differ" do
      dots = [{"a", 1}]
      context1 = Context.new(dots)
      context2 = Context.new([{"b", 42}] ++ dots)
      refute Context.eq?(context1, context2)
    end

    test "ignores ordering of dots" do
      context1 = Context.new([{"a", 1}, {"b", 2}, {"a", 3}])
      context2 = Context.new([{"a", 3}, {"b", 2}, {"a", 1}])

      assert Context.eq?(context1, context2)
    end
  end

  describe "lt/2" do
    test "returns false on empty contexts" do
      refute Context.lt?(Context.new(), Context.new())
    end

    test "returns false if two contexts are equal" do
      dots = [{"a", 1}, {"b", 42}]
      context1 = Context.new(dots)
      context2 = Context.new(dots)
      refute Context.lt?(context1, context2)
    end

    test "returns true if the first context is a strict subset of the second" do
      dots = [{"a", 1}]
      context1 = Context.new(dots)
      context2 = Context.new([{"a", 2} | dots])
      assert Context.lt?(context1, context2)
    end

    test "returns false if the first context is greater than the second" do
      dots = [{"a", 1}]
      context1 = Context.new(dots)
      context2 = Context.new([{"a", 2} | dots])
      refute Context.lt?(context2, context1)
    end

    test "considers dots grouped by replica" do
      context1 = Context.new([{"a", 1}, {"b", 1}])
      context2 = Context.new([{"a", 1}, {"a", 2}, {"b", 1}])

      assert Context.lt?(context1, context2)
    end

    test "treats empty as smaller than non-empty" do
      refute Context.lt?(Context.new(), Context.new())
      assert Context.lt?(Context.new(), Context.new([{"a", 1}]))
    end

    test "returns false when a replica is missing progress" do
      left = Context.new([{"a", 1}])
      right = Context.new([{"b", 1}])

      refute Context.lt?(left, right)
      refute Context.lt?(right, left)
    end
  end

  describe "delta_since/2" do
    test "when called on empty context always returns empty context" do
      assert Context.delta_since(Context.new(), Context.new()) |> Context.empty?()
      assert Context.delta_since(Context.new(), Context.new([{"a", 1}])) |> Context.empty?()
    end

    test "when called with empty since-context returns original context" do
      dots = [{"a", 1}]
      context1 = Context.new(dots)
      assert Context.delta_since(context1, Context.new()) |> Context.eq?(context1)
    end

    test "returns dots not present in since-context" do
      base = [{"a", 1}]
      ctx_dot = [{"b", 2}]
      since_dot = [{"b", 3}]
      ctx = Context.new(base ++ ctx_dot)
      since = Context.new(base ++ since_dot)
      assert Context.delta_since(ctx, since) |> Context.eq?(Context.new(ctx_dot))
    end

    test "drops empty replica entries from the delta" do
      ctx = Context.new([{"a", 1}, {"b", 2}])
      since = Context.new([{"a", 1}])

      assert Context.delta_since(ctx, since) |> Context.eq?(Context.new([{"b", 2}]))
    end

    test "ignores dots that are only present in since-context" do
      ctx = Context.new([{"a", 1}])
      since = Context.new([{"a", 1}, {"b", 2}])

      assert Context.delta_since(ctx, since) |> Context.empty?()
    end

    test "returns empty when since is ahead on the same replica" do
      ctx = Context.new([{"a", 1}])
      since = Context.new([{"a", 1}, {"a", 2}])

      assert Context.delta_since(ctx, since) |> Context.empty?()
    end
  end

  describe "join/2" do
    test "unions dots from both contexts" do
      ctx_a = Context.new([{"a", 1}])
      ctx_b = Context.new([{"a", 2}, {"b", 1}])

      assert Context.join(ctx_a, ctx_b)
             |> Context.eq?(Context.new([{"a", 1}, {"a", 2}, {"b", 1}]))
    end

    test "is commutative and idempotent" do
      ctx_a = Context.new([{"a", 1}])
      ctx_b = Context.new([{"a", 2}, {"b", 1}])

      assert Context.join(ctx_a, ctx_b) |> Context.eq?(Context.join(ctx_b, ctx_a))
      assert Context.join(ctx_a, ctx_a) |> Context.eq?(ctx_a)
    end
  end
end
