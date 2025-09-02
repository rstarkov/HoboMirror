using System.Runtime.InteropServices;

namespace HoboMirror;

static class ExtensionMethods
{
    public static string WithSlash(this string path)
    {
        return path.EndsWith("\\") ? path : (path + "\\");
    }
}

static unsafe class Ptr
{
    public static int Diff<T>(void* p1, void* p2) where T : unmanaged => (int)((T*)p1 - (T*)p2);
    public static Span<T> SpanFromPtrStartEnd<T>(void* start, void* end) where T : unmanaged => new Span<T>(start, Diff<T>(end, start));
    public static Span<T> SpanFromPtrAndByteLength<T>(void* start, int byteLen) where T : unmanaged => new Span<T>(start, byteLen / sizeof(T));

    public static ReadOnlySpan<char> SpanFromNullStr(char* str) => MemoryMarshal.CreateReadOnlySpanFromNullTerminated(str);
    public static ReadOnlySpan<char> SpanFromNullStr(Span<char> str)
    {
        fixed (char* p = str)
            return MemoryMarshal.CreateReadOnlySpanFromNullTerminated(p);
    }
}
