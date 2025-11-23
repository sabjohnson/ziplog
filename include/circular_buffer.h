template<typename T>
class CircularBuffer {
    vector<T> buffer_;
    size_t head_ = 0;
    size_t tail_ = 0;
    size_t size_ = 0;
    size_t capacity_;
    mutex mu_;

public:
    CircularBuffer(size_t capacity) : capacity_(capacity) {
        buffer_.resize(capacity);
    }

    bool push(const T& item) {
        lock_guard<mutex> lock(mu_);
        if (size_ == capacity_) return false;  // Full

        buffer_[tail_] = item;
        tail_ = (tail_ + 1) % capacity_;
        size_++;
        return true;
    }

    bool pop(T& item) {
        lock_guard<mutex> lock(mu_);
        if (size_ == 0) return false;  // Empty

        item = buffer_[head_];
        head_ = (head_ + 1) % capacity_;
        size_--;
        return true;
    }

    vector<T> drain_all() {
        lock_guard<mutex> lock(mu_);
        vector<T> result;
        while (size_ > 0) {
            result.push_back(buffer_[head_]);
            head_ = (head_ + 1) % capacity_;
            size_--;
        }
        return result;
    }
};

// In Proxy:
CircularBuffer<Command> pending_commands_(1000);
CircularBuffer<int> pending_clients_(1000);