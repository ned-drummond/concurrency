CXX      := g++
CXXFLAGS := -std=c++20 -Wall -Wextra
TARGET   := main

all: $(TARGET)

$(TARGET): main.cpp
	$(CXX) $(CXXFLAGS) -o $@ $<

clean:
	rm -f $(TARGET)

.PHONY: all clean